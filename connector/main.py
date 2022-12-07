import asyncio
import logging
import sys
from typing import Dict, Optional, Tuple

import asyncio_mqtt as aiomqtt
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Message
from kafka.errors import KafkaConnectionError

from connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_HEADERS_TEMPLATE,
    KAFKA_TOPIC_TEMPLATE,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_RECONNECT_INTERVAL_SEC,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_TOPIC_SOURCE_TEMPLATE,
    MQTT_USER,
)
from connector.utils import Template

logger = logging.getLogger(__name__)


Kafka_headers_type = Dict[str, bytes]
Topic_headers = Tuple[str, Kafka_headers_type]


def get_kafka_producer_params(mqtt_topic: str) -> Optional[Topic_headers]:
    """Получить топик и заголовки Kafka из топика MQTT"""
    tpl = Template(MQTT_TOPIC_SOURCE_TEMPLATE)
    if headers := tpl.to_dict(mqtt_topic):
        # return (tpl.to_topic(), headers)
        header_names = KAFKA_HEADERS_TEMPLATE.split('|')
        kafka_headers = [
            (k, v.encode('utf-8'))
            for k, v in headers.items()
            if k in header_names
        ]
        kafka_topic = KAFKA_TOPIC_TEMPLATE.format(**headers)
        return (kafka_topic, kafka_headers)
    return None


async def send_to_kafka(
    topic: str, value: bytes, headers: Dict = None
) -> bool:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        await producer.start()
        await producer.send(topic, value, headers=headers)
        logger.info(f'Send to kafka {topic=}, {value=}')
        return True
    except KafkaConnectionError as e:
        logger.error(f'Kafka sending error {e}')
        return False
    finally:
        await producer.stop()


async def mqtt_message_handler(message: Message) -> bool:
    mqtt_topic = message.topic
    logger.info(
        f'Message received from {mqtt_topic.value=} '
        f'{message.payload=} {message.qos=}'
    )
    res = get_kafka_producer_params(mqtt_topic.value)
    if res:
        kafka_topic, kafka_headers = res
        res = await send_to_kafka(
            kafka_topic,
            message.payload,
            headers=kafka_headers,
        )
        return res
    else:
        logger.warning(f'Error prepare kafka topic from {mqtt_topic.value=}')
        return False


async def run():
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
                        await mqtt_message_handler(message)

        except aiomqtt.MqttError as error:
            logger.warning(
                f'Error {error=}. '
                f'Reconnecting in {MQTT_RECONNECT_INTERVAL_SEC} seconds.'
            )
            await asyncio.sleep(MQTT_RECONNECT_INTERVAL_SEC)


def main():
    asyncio.run(run())


if __name__ == '__main__':
    sys.exit(main())
