import asyncio
import logging
import typing

import aiomqtt

from mqtt_kafka_connector import conf

logger = logging.getLogger(__name__)


class MQTTClient:
    def __init__(self):
        self.client = None
        self.loop = None

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.client = aiomqtt.Client(
            hostname=conf.MQTT_HOST,
            port=conf.MQTT_PORT,
            username=conf.MQTT_USER,
            password=conf.MQTT_PASSWORD,
            identifier=conf.MQTT_CLIENT_ID,
            clean_session=False,
            timeout=300,
        )
        # setup manual ack
        self.loop.run_in_executor(None, self.client._client.manual_ack_set, True)
        logger.info("MQTT Client is running")

    async def get_messages(self) -> typing.AsyncIterator[aiomqtt.Message]:
        if self.client is None:
            raise RuntimeError("Client is not initialized")

        async with self.client as cli:
            await cli.subscribe(conf.MQTT_TOPIC_SOURCE_MATCH, qos=1)  # customer/#
            await cli.subscribe(conf.MQTT_FSTATE_SOURCE_MATCH, qos=1)  # fstate/#

            async for mqtt_message in cli.messages:
                yield mqtt_message
                # send ack
                self.loop.run_in_executor(
                    None,
                    cli._client.ack,
                    mqtt_message.mid,
                    mqtt_message.qos,
                )
