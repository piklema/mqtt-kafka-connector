import asyncio
import io
import json
import logging
import sys

import aiomqtt
import fastavro
import orjson
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from mqtt_kafka_connector.clients.schema_client import schema_client
from mqtt_kafka_connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_HEADERS_LIST,
    KAFKA_KEY_TEMPLATE,
    MESSAGE_DESERIALIZE,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_TOPIC_SOURCE_TEMPLATE,
    MQTT_USER,
    RECONNECT_INTERVAL_SEC,
    TELEMETRY_KAFKA_TOPIC,
    TRACE_HEADER,
    MODIFY_MESSAGE_RM_NONE_FIELDS,
    MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS,
)
from mqtt_kafka_connector.context_vars import message_uuid_var, setup_context_vars
from mqtt_kafka_connector.utils import Template, clean_none_fields

logger = logging.getLogger(__name__)

KafkaHeadersType = list[tuple[str, bytes]]
TopicHeaders = tuple[str, bytes, KafkaHeadersType]


class Connector:
    def __init__(self, message_deserialize: bool = False):
        self.mqtt_topic_params_tmpl = Template(MQTT_TOPIC_SOURCE_TEMPLATE)
        self.header_names = KAFKA_HEADERS_LIST.split(',')
        self.message_deserialize = message_deserialize
        self.schema_client = schema_client
        self.producer: AIOKafkaProducer = None

    def get_kafka_producer_params(
        self,
        mqtt_topic: str,
    ) -> TopicHeaders | None:
        """Get Kafka topic & headers from MQTT topic"""
        if mqtt_topic_params := self.mqtt_topic_params_tmpl.to_dict(mqtt_topic):
            kafka_topic = TELEMETRY_KAFKA_TOPIC.format(**mqtt_topic_params)
            kafka_key = KAFKA_KEY_TEMPLATE.format(**mqtt_topic_params).encode()
            kafka_headers = [(k, v.encode()) for k, v in mqtt_topic_params.items() if k in self.header_names]
            return kafka_topic, kafka_key, kafka_headers

    async def send_to_kafka(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: list = None,
    ) -> bool:
        await self.producer.send(topic, value=value, key=key, headers=headers)
        logger.info('Send to kafka %s %s %s %s', topic, key, value, headers)
        return True

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

        logger.info(f"Message deserialized: {data=}")

        return data

    async def mqtt_message_handler(self, message: aiomqtt.Message) -> bool:
        mqtt_topic = message.topic
        logger.info(
            f'Message received from {mqtt_topic.value=} ' f'{message.payload=} {message.qos=}',
        )
        res = self.get_kafka_producer_params(mqtt_topic.value)
        if res:
            kafka_topic, kafka_key, kafka_headers = res

            if self.message_deserialize:
                kafka_headers.append(('message_deserialized', b'1'))
                schema_id = int(dict(kafka_headers)['schema_id'])
                msg_dict = await self.deserialize(message, schema_id)
                messages = msg_dict['messages']

            else:
                data = message.payload
                messages = json.loads(data.decode())['messages']
                # messages = [ for message in messages]

            if TRACE_HEADER:
                kafka_headers.append((TRACE_HEADER, message_uuid_var.get().encode()))

            res = []
            for message in messages:
                if MODIFY_MESSAGE_RM_NONE_FIELDS:
                    message = clean_none_fields(message)

                """ Неявное приведение к стандарту JSON без NaN, Inf, -Inf Значений через orjson),"""
                json_for_kafka = (
                    orjson.dumps(message) if MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS else json.dumps(message).encode()
                )

                res.append(
                    await self.send_to_kafka(
                        kafka_topic,
                        value=json_for_kafka,
                        key=kafka_key,
                        headers=kafka_headers,
                    ),
                )
            return all(res)
        else:
            logger.warning(f'Error prepare kafka topic from {mqtt_topic.value=}')
            return False

    async def run(self):
        logger.info('MQTT Kafka connector starting...')
        while True:
            try:
                self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
                await self.producer.start()
                logger.info('Kafka Producer is running')
                async with aiomqtt.Client(
                    hostname=MQTT_HOST,
                    port=MQTT_PORT,
                    username=MQTT_USER,
                    password=MQTT_PASSWORD,
                    client_id=MQTT_CLIENT_ID,
                    clean_session=False,
                ) as client:
                    logger.info('MQTT Client is running')
                    async with client.messages() as messages:
                        await client.subscribe(MQTT_TOPIC_SOURCE_MATCH, qos=1)
                        async for message in messages:
                            try:
                                mqtt_params = self.mqtt_topic_params_tmpl.to_dict(message.topic.value)
                                setup_context_vars(mqtt_params.get('device_id'))
                                await self.mqtt_message_handler(message)
                            except RuntimeError as error:
                                logger.error('Runtime error: %s', error)

            except aiomqtt.MqttError as error:
                logger.warning(
                    'MQTT connection error %s. ' 'Reconnecting in %s seconds.',
                    error,
                    RECONNECT_INTERVAL_SEC,
                )
                await asyncio.sleep(RECONNECT_INTERVAL_SEC)
            except KafkaConnectionError as error:
                logger.warning(
                    'Kafka connection error %s. ' 'Reconnecting in %s seconds.',
                    error,
                    RECONNECT_INTERVAL_SEC,
                )
                await asyncio.sleep(RECONNECT_INTERVAL_SEC)
            finally:
                await self.producer.stop()


def main():
    conn = Connector(message_deserialize=MESSAGE_DESERIALIZE)
    asyncio.run(conn.run())


if __name__ == '__main__':
    sys.exit(main())
