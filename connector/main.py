import asyncio
import logging
import sys

import asyncio_mqtt as aiomqtt
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Message
from kafka.errors import KafkaConnectionError

from connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_RECONNECT_INTERVAL_SEC,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)
from connector.utils import prepare_topic_mqtt_to_kafka

logger = logging.getLogger(__name__)


async def send_to_kafka(topic: str, value: bytes) -> bool:

    try:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
    except KafkaConnectionError as e:
        logger.error(f'Kafka sending error {e}')
        return False

    try:
        await producer.send_and_wait(topic, value)
        logger.info(f'Send to kafka {topic=}, {value=}')
        return True
    finally:
        await producer.stop()


async def mqtt_message_handler(message: Message) -> bool:
    mqtt_topic = message.topic
    logger.info(
        f'Message received from {mqtt_topic.value=} '
        f'{message.payload=} {message.qos=}'
    )

    if mqtt_topic.matches(MQTT_TOPIC_SOURCE_MATCH):
        res = await send_to_kafka(
            prepare_topic_mqtt_to_kafka(mqtt_topic.value),
            message.payload,
        )
    else:
        res = False
        logger.warning(
            f'Message {message.payload=} '
            f'receive from wrong topic: {mqtt_topic.value=}'
        )
    return res


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
                    await client.subscribe('#', qos=2)
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
