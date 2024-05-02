import asyncio
import io
import json
import logging
import sys
from collections import defaultdict

import aiomqtt
import fastavro
from aiokafka.errors import KafkaConnectionError
from aiomqtt.message import Message
from aioprometheus import Counter
from aioprometheus.service import Service

from mqtt_kafka_connector.clients.kafka import KafkaProducer
from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.clients.schema_client import SchemaClient
from mqtt_kafka_connector.conf import (
    KAFKA_HEADERS_LIST,
    KAFKA_KEY_TEMPLATE,
    KAFKA_SEND_BATCHES,
    MQTT_TOPIC_SOURCE_TEMPLATE,
    RECONNECT_INTERVAL_SEC,
    TELEMETRY_KAFKA_TOPIC,
    TRACE_HEADER,
    WITH_MESSAGE_DESERIALIZE,
)
from mqtt_kafka_connector.context_vars import (
    message_uuid_var,
    setup_context_vars,
)
from mqtt_kafka_connector.utils import Template

logger = logging.getLogger(__name__)

KafkaHeadersType = list[tuple[str, bytes]]
TopicHeaders = tuple[str, bytes, KafkaHeadersType]


class Connector:
    def __init__(
        self,
        mqtt_client: MQTTClient,
        kafka_producer: KafkaProducer,
        schema_client: SchemaClient,
    ):
        self.mqtt_topic_params_tmpl = Template(MQTT_TOPIC_SOURCE_TEMPLATE)
        self.kafka_producer = kafka_producer
        self.mqtt_client = mqtt_client
        self.schema_client = schema_client
        self.last_messages = defaultdict(dict)

        self.prometheus_service = Service()
        self.messages_counter = Counter('messages_count', 'Number of messages.')

    async def deserialize(self, msg: aiomqtt.Message, schema_id: int) -> dict:
        schema = await self.schema_client.get_schema(schema_id)

        if not schema:
            raise RuntimeError('Schema not found')

        fp = io.BytesIO(msg.payload)
        try:
            parsed_schema = fastavro.parse_schema(schema)
            data = fastavro.schemaless_reader(fp, parsed_schema)
        except (IndexError, StopIteration, EOFError):
            raise RuntimeError('Message is not valid')

        logger.debug('Message deserialized: data=%s', data)

        return data

    def check_telemetry_messages_pack(
        self, mqtt_topic: str, telemetry_msg_pack: list
    ) -> bool:
        last_message = telemetry_msg_pack[-1]
        messages_count = len(telemetry_msg_pack)

        if self.last_messages[mqtt_topic] != last_message:
            self.last_messages[mqtt_topic] = last_message
        else:
            logger.info(
                'Message pack from %s already sending. Skip sending to kafka',
                mqtt_topic,
            )
            return False

        logger.info('Receive %s messages from %s', messages_count, mqtt_topic)
        return True

    async def get_telemetry_message_pack(
        self,
        mqtt_message: aiomqtt.Message,
        schema_id: int,
    ) -> list[dict] | None:
        mqtt_topic = mqtt_message.topic

        logger.debug(
            'Message received from '
            'mqtt_topic.value=%s message.payload=%s message.qos=%s',
            mqtt_topic.value,
            mqtt_message.payload,
            mqtt_message.qos,
        )

        if WITH_MESSAGE_DESERIALIZE:
            mqtt_msg_dict = await self.deserialize(mqtt_message, schema_id)
            telemetry_msg_pack = mqtt_msg_dict['messages']

        else:
            data = mqtt_message.payload
            telemetry_msg_pack = json.loads(data.decode())['messages']

        if not telemetry_msg_pack:
            logger.warning('Messages is empty')
            return

        return telemetry_msg_pack

    @staticmethod
    def get_kafka_message_params(
        mqtt_topic_params: dict,
    ) -> TopicHeaders | None:
        """Get Kafka topic & headers from MQTT topic"""
        kafka_topic = TELEMETRY_KAFKA_TOPIC.format(**mqtt_topic_params)
        kafka_key = KAFKA_KEY_TEMPLATE.format(**mqtt_topic_params).encode()
        kafka_headers = [
            (k, v.encode())
            for k, v in mqtt_topic_params.items()
            if k in KAFKA_HEADERS_LIST.split(',')
        ]

        if WITH_MESSAGE_DESERIALIZE:
            kafka_headers.append(('message_deserialized', b'1'))

        if TRACE_HEADER:
            kafka_headers.append(
                (TRACE_HEADER, message_uuid_var.get().encode())
            )

        return kafka_topic, kafka_key, kafka_headers

    async def kafka_handler(
        self,
        messages: list,
        kafka_topic: str,
        kafka_key: bytes,
        kafka_headers: KafkaHeadersType,
    ):
        logger.info(
            'Start send to kafka topic=%s, key=%s', kafka_topic, int(kafka_key)
        )

        if KAFKA_SEND_BATCHES:
            await self.kafka_producer.send_batch(
                kafka_topic,
                messages,
                kafka_key,
                kafka_headers,
            )

        else:
            for msg in messages:
                await self.kafka_producer.send(
                    kafka_topic,
                    message=msg,
                    key=kafka_key,
                    headers=kafka_headers,
                )

        return True

    async def run(self):
        logger.info('MQTT Kafka connector starting...')
        while True:
            try:
                await self.kafka_producer.start()
                logger.info('Kafka Producer is running')

                await self.prometheus_service.start(addr='0.0.0.0', port=8011)
                logger.info('Prometheus service is running')

                async for mqtt_message in self.mqtt_client.get_messages():
                    try:
                        await self.handle(mqtt_message)
                    except RuntimeError as err:
                        logger.error('Runtime error: %s', err)

            except aiomqtt.MqttError as err:
                logger.warning(
                    'MQTT connection error %s. ' 'Reconnecting in %s seconds.',
                    err,
                    RECONNECT_INTERVAL_SEC,
                )
                await asyncio.sleep(RECONNECT_INTERVAL_SEC)
            except KafkaConnectionError as err:
                logger.warning(
                    'Kafka connection error %s. '
                    'Reconnecting in %s seconds.',
                    err,
                    RECONNECT_INTERVAL_SEC,
                )
                await asyncio.sleep(RECONNECT_INTERVAL_SEC)
            finally:
                await self.kafka_producer.stop()
                await self.prometheus_service.stop()

    async def handle(self, mqtt_message: Message):
        mqtt_topic = mqtt_message.topic.value
        mqtt_params = self.mqtt_topic_params_tmpl.to_dict(
            mqtt_message.topic.value
        )
        device_id = mqtt_params.get('device_id')
        setup_context_vars(device_id)

        kafka_topic, kafka_key, kafka_headers = self.get_kafka_message_params(
            mqtt_params
        )
        schema_id = int(dict(kafka_headers)['schema_id'])
        telemetry_msg_pack = await self.get_telemetry_message_pack(
            mqtt_message, schema_id
        )
        if self.check_telemetry_messages_pack(mqtt_topic, telemetry_msg_pack):

            await self.kafka_handler(
                telemetry_msg_pack, kafka_topic, kafka_key, kafka_headers
            )

            self.messages_counter.add(
                {
                    'device_id': device_id,
                    'customer_id': mqtt_params.get('customer_id'),
                },
                len(telemetry_msg_pack),
            )

        return True


def main():
    loop = asyncio.get_event_loop()

    producer = KafkaProducer(loop)
    mqtt_client = MQTTClient()
    schema_client = SchemaClient()

    connector = Connector(mqtt_client, producer, schema_client)
    asyncio.run(connector.run())


if __name__ == '__main__':
    sys.exit(main())
