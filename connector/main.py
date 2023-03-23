import asyncio
import io
import json
import logging
import sys
import uuid
from functools import lru_cache
from typing import List, Optional, Tuple

import asyncio_mqtt as aiomqtt
import fastavro
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Message
from kafka.errors import KafkaConnectionError

from connector.clients.schema_client import schema_client
from connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_HEADERS_LIST,
    KAFKA_TOPIC_TEMPLATE,
    MESSAGE_DESERIALIZE,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_RECONNECT_INTERVAL_SEC,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_TOPIC_SOURCE_TEMPLATE,
    MQTT_USER,
    TRACE_HEADER,
)
from connector.utils import Template

logger = logging.getLogger(__name__)


KafkaHeadersType = List[Tuple[str, bytes]]
TopicHeaders = Tuple[str, KafkaHeadersType]


class Connector:
    def __init__(self, message_deserialize: bool = False):
        self.tpl = Template(MQTT_TOPIC_SOURCE_TEMPLATE)
        self.header_names = KAFKA_HEADERS_LIST.split(',')
        self.message_deserialize = message_deserialize
        self.schema_client = schema_client

    def get_kafka_producer_params(
        self,
        mqtt_topic: str,
    ) -> Optional[TopicHeaders]:
        """Get Kafka topic & headers from MQTT topic"""
        if headers := self.tpl.to_dict(mqtt_topic):
            kafka_topic = KAFKA_TOPIC_TEMPLATE.format(**headers)
            kafka_headers = [
                (k, v.encode('utf-8'))
                for k, v in headers.items()
                if k in self.header_names
            ]
            return kafka_topic, kafka_headers
        return None

    @staticmethod
    async def send_to_kafka(
        topic: str,
        value: bytes,
        headers: List = None,
    ) -> bool:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        try:
            await producer.start()
            await producer.send(topic, value, headers=headers)
            logger.info(f'Send to kafka {topic=}, {value=}, {headers=}')
            return True
        except KafkaConnectionError as e:
            logger.error(f'Kafka sending error {e}')
            return False
        finally:
            await producer.stop()

    @lru_cache()
    async def get_schema(self, schema_id) -> dict:
        return await self.schema_client.get_schema(schema_id)

    async def deserialize(self, msg: Message, schema_id: int) -> dict:
        schema = await self.get_schema(schema_id)

        if not schema:
            raise RuntimeError('Schema not found')

        fp = io.BytesIO(msg.payload)
        try:
            data = fastavro.schemaless_reader(fp, schema)
        except (IndexError, StopIteration):
            raise RuntimeError('Message is not valid')

        logger.info(f"Message deserialized: {data=}")

        return data

    async def mqtt_message_handler(self, message: Message) -> bool:
        message_uuid = uuid.uuid4().hex.encode()
        mqtt_topic = message.topic
        logger.info(
            f'Message received from {mqtt_topic.value=} '
            f'{message.payload=} {message.qos=}',
            extra={TRACE_HEADER: message_uuid},
        )
        res = self.get_kafka_producer_params(mqtt_topic.value)
        if res:
            kafka_topic, kafka_headers = res

            if self.message_deserialize:
                schema_id = int(dict(kafka_headers)['schema_id'])
                msg_dict = await self.deserialize(message, schema_id)
                data = json.dumps(msg_dict).encode()
            else:
                data = message.payload

            if TRACE_HEADER:
                kafka_headers.append((TRACE_HEADER, message_uuid))

            res = await self.send_to_kafka(
                kafka_topic,
                data,
                headers=kafka_headers,
            )
            return res
        else:
            logger.warning(
                f'Error prepare kafka topic from {mqtt_topic.value=}'
            )
            return False

    async def run(self):
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
                            await self.mqtt_message_handler(message)

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
