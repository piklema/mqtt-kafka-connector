import asyncio
import logging

import asyncio_mqtt as aiomqtt
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Message

from conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    MQTT_HOST,
    MQTT_PORT,
    MQTT_CLIENT_ID,
    MQTT_PASSWORD,
    MQTT_RECONNECT_INTERVAL_SEC,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)
from utils import prepare_topic_mqtt_to_kafka
from kafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)


async def send_to_kafka(topic: str, value: bytes):

    try:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
    except KafkaConnectionError as e:
        logger.error(f'Kafka sending error {e}')
        return

    try:
        await producer.send_and_wait(topic, value)
        logger.info(f'Send to kafka {topic=}, {value=}')
    finally:
        await producer.stop()


async def handler(message: Message):
    mqtt_topic = message.topic
    logger.info(
        f'Message received from {mqtt_topic.value=} '
        f'{message.payload=} {message.qos=}'
    )
    if mqtt_topic.matches(MQTT_TOPIC_SOURCE_MATCH):
        await send_to_kafka(
            prepare_topic_mqtt_to_kafka(mqtt_topic.value),
            message.payload,
        )
    else:
        logger.warning(
            f'Message {message.payload=} '
            f'receive from wrong topic: {mqtt_topic.value=}'
        )


async def main():
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
                    await client.subscribe('#', qos=2)
                    async for message in messages:
                        await handler(message)

        except aiomqtt.MqttError as error:
            logger.warning(
                f'Error {error=}. '
                f'Reconnecting in {MQTT_RECONNECT_INTERVAL_SEC} seconds.'
            )
            await asyncio.sleep(MQTT_RECONNECT_INTERVAL_SEC)


if __name__ == '__main__':
    asyncio.run(main())
