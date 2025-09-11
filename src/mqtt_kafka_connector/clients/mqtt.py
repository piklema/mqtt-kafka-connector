import asyncio
import logging
import typing

import aiomqtt

from mqtt_kafka_connector.settings import settings

logger = logging.getLogger(__name__)


class MQTTClient:
    def __init__(self):
        self.client = None
        self.loop = None

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.client = aiomqtt.Client(
            hostname=settings.MQTT_HOST,
            port=settings.MQTT_PORT,
            username=settings.MQTT_USER,
            password=settings.MQTT_PASSWORD,
            identifier=settings.MQTT_CLIENT_ID,
            clean_session=True,
            timeout=300,
        )
        # настраиваем ручное подтверждение
        self.loop.run_in_executor(None, self.client._client.manual_ack_set, True)
        logger.info("Клиент MQTT запущен")

    async def get_messages(self) -> typing.AsyncIterator[aiomqtt.Message]:
        if self.client is None:
            raise RuntimeError("Клиент не инициализирован")

        async with self.client as cli:
            await cli.subscribe(settings.MQTT_TOPIC_SOURCE_MATCH, qos=1)  # customer/#
            await cli.subscribe(settings.MQTT_FSTATE_SOURCE_MATCH, qos=1)  # fstate/#

            async for mqtt_message in cli.messages:
                yield mqtt_message
                # отправляем подтверждение
                self.loop.run_in_executor(
                    None,
                    cli._client.ack,
                    mqtt_message.mid,
                    mqtt_message.qos,
                )
