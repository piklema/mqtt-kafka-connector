import asyncio
import logging
import typing

import aiomqtt

from mqtt_kafka_connector.conf import (
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)

logger = logging.getLogger(__name__)


class MQTTClient:
    def __init__(self):
        self.client = None
        self.loop = None

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.client = aiomqtt.Client(
            hostname=MQTT_HOST,
            port=MQTT_PORT,
            username=MQTT_USER,
            password=MQTT_PASSWORD,
            identifier=MQTT_CLIENT_ID,
            clean_session=False,
            timeout=300,
        )
        # setup manual ack
        self.loop.run_in_executor(
            None, self.client._client.manual_ack_set, True
        )
        logger.info('MQTT Client is running')

    async def get_messages(self) -> typing.AsyncIterator[aiomqtt.Message]:
        async with self.client as cli:
            await cli.subscribe(MQTT_TOPIC_SOURCE_MATCH, qos=1)
            async for mqtt_message in cli.messages:
                yield mqtt_message
                # send ack
                self.loop.run_in_executor(
                    None,
                    cli._client.ack,
                    mqtt_message.mid,
                    mqtt_message.qos,
                )
