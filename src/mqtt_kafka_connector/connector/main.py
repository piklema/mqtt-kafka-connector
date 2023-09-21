import asyncio
import io
import json
import logging
import sys
from typing import List, Optional, Tuple

import asyncio_mqtt as aiomqtt
import fastavro
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Message
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
    MQTT_RECONNECT_INTERVAL_SEC,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_TOPIC_SOURCE_TEMPLATE,
    MQTT_USER,
    TELEMETRY_KAFKA_TOPIC,
    TRACE_HEADER,
)
from mqtt_kafka_connector.context_vars import message_uuid_var, setup_vars
from mqtt_kafka_connector.utils import DateTimeEncoder, Template

logger = logging.getLogger(__name__)


KafkaHeadersType = List[Tuple[str, bytes]]
TopicHeaders = Tuple[str, bytes, KafkaHeadersType]


class Connector:
    def __init__(self, message_deserialize: bool = False):
        self.mqtt_topic_params_tmpl = Template(MQTT_TOPIC_SOURCE_TEMPLATE)
        self.header_names = KAFKA_HEADERS_LIST.split(',')
        self.message_deserialize = message_deserialize
        self.schema_client = schema_client

    def get_kafka_producer_params(
        self,
        mqtt_topic: str,
    ) -> Optional[TopicHeaders]:
        """Get Kafka topic & headers from MQTT topic"""
        if mqtt_topic_params := self.mqtt_topic_params_tmpl.to_dict(
            mqtt_topic
        ):
            kafka_topic = TELEMETRY_KAFKA_TOPIC.format(**mqtt_topic_params)
            kafka_key = KAFKA_KEY_TEMPLATE.format(**mqtt_topic_params).encode()
            kafka_headers = [
                (k, v.encode())
                for k, v in mqtt_topic_params.items()
                if k in self.header_names
            ]
            return kafka_topic, kafka_key, kafka_headers

    @staticmethod
    async def send_to_kafka(
        topic: str,
        value: bytes,
        key: bytes,
        headers: List = None,
    ) -> bool:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        try:
            await producer.start()
            await producer.send(topic, value=value, key=key, headers=headers)
            logger.info(
                f'Send to kafka {topic=}, {key=}, {value=}, {headers=}'
            )
            return True
        except KafkaConnectionError as e:
            logger.error(f'Kafka sending error {e}')
            return False
        finally:
            await producer.stop()

    async def deserialize(self, msg: Message, schema_id: int) -> dict:
        schema = await self.schema_client.get_schema(schema_id)

        if not schema:
            raise RuntimeError('Schema not found')

        fp = io.BytesIO(msg.payload)
        try:
            parsed_schema = fastavro.parse_schema(schema)
            data = fastavro.schemaless_reader(fp, parsed_schema)
        except (IndexError, StopIteration):
            raise RuntimeError('Message is not valid')

        logger.info(f"Message deserialized: {data=}")

        return data

    async def mqtt_message_handler(self, message: Message) -> bool:
        mqtt_topic = message.topic
        logger.info(
            f'Message received from {mqtt_topic.value=} '
            f'{message.payload=} {message.qos=}',
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

            if TRACE_HEADER:
                kafka_headers.append((TRACE_HEADER, message_uuid_var.get()))

            res = []
            for message in messages:
                res.append(
                    await self.send_to_kafka(
                        kafka_topic,
                        value=json.dumps(
                            message, cls=DateTimeEncoder
                        ).encode(),
                        key=kafka_key,
                        headers=kafka_headers,
                    )
                )
            return all(res)
        else:
            logger.warning(
                f'Error prepare kafka topic from {mqtt_topic.value=}'
            )
            return False

    async def run(self, single_run: bool = False):
        logger.info('MQTT Kafka connector is running')
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=MQTT_HOST,
                    port=MQTT_PORT,
                    username=MQTT_USER,
                    password=MQTT_PASSWORD,
                    client_id=MQTT_CLIENT_ID,
                    clean_session=False,
                ) as client:
                    async with client.messages() as messages:
                        await client.subscribe(MQTT_TOPIC_SOURCE_MATCH, qos=2)
                        async for message in messages:
                            try:
                                mqtt_params = (
                                    self.mqtt_topic_params_tmpl.to_dict(
                                        message.topic.value
                                    )
                                )
                                setup_vars(mqtt_params.get('device_id'))
                                await self.mqtt_message_handler(message)
                            except RuntimeError as e:
                                logger.error(f'Runtime error: {e}')

                if single_run:
                    return

            except aiomqtt.MqttError as error:
                logger.warning(
                    f'Error {error=}. '
                    f'Reconnecting in {MQTT_RECONNECT_INTERVAL_SEC} seconds.'
                )
                await asyncio.sleep(MQTT_RECONNECT_INTERVAL_SEC)


def main():
    conn = Connector(message_deserialize=MESSAGE_DESERIALIZE)
    asyncio.run(conn.run())


if __name__ == '__main__':
    sys.exit(main())
