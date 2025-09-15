from __future__ import annotations

import asyncio
import logging

import aiomqtt
from aiokafka.errors import KafkaConnectionError
from aiomqtt.message import Message

from mqtt_kafka_connector.clients.kafka import KafkaProducer
from mqtt_kafka_connector.connector.handlers import TopicRouter
from mqtt_kafka_connector.services.prometheus import Prometheus
from mqtt_kafka_connector.settings import settings

logger = logging.getLogger(__name__)


class Connector:
    """
    Основной класс коннектора, который связывает MQTT и Kafka.
    """

    def __init__(
        self,
        mqtt_client: aiomqtt.Client,
        kafka_producer: KafkaProducer,
        topic_router: TopicRouter,
        prometheus: Prometheus | None = None,
    ):
        """
        Инициализация коннектора.

        Args:
            mqtt_client: Клиент для подключения к MQTT.
            kafka_producer: Продюсер для отправки сообщений в Kafka.
            topic_router: Маршрутизатор для обработки сообщений.
            prometheus: Сервис для сбора метрик.
        """
        self.mqtt_client = mqtt_client
        self.kafka_producer = kafka_producer
        self.topic_router = topic_router
        self.prometheus = prometheus

    async def run(self):
        """
        Основной цикл работы коннектора.
        """
        logger.info("Запуск коннектора...")
        while True:
            try:
                if self.prometheus:
                    await self.prometheus.start()
                await self.mqtt_client.start()
                await self.kafka_producer.start()

                async for mqtt_message in self.mqtt_client.get_messages():
                    try:
                        await self.handle(mqtt_message)
                    except RuntimeError as err:
                        logger.error("Ошибка выполнения: %s", err)

            except aiomqtt.MqttError as err:
                logger.warning(
                    "Ошибка подключения к MQTT %r. Повторное подключение через %r секунд (%r).",
                    err,
                    settings.RECONNECT_INTERVAL_SEC,
                    str(self.mqtt_client),
                )
                await asyncio.sleep(settings.RECONNECT_INTERVAL_SEC)
            except KafkaConnectionError as err:
                logger.warning(
                    "Ошибка подключения к Kafka %s. Повторное подключение через %r секунд.",
                    err,
                    settings.RECONNECT_INTERVAL_SEC,
                )
                await asyncio.sleep(settings.RECONNECT_INTERVAL_SEC)
            finally:
                await self.kafka_producer.stop()
                if self.prometheus:
                    await self.prometheus.service.stop()

    async def handle(self, mqtt_message: Message) -> bool:
        """
        Обработка входящего сообщения из MQTT.

        Args:
            mqtt_message: Сообщение из MQTT.

        Returns:
            True, если сообщение было успешно обработано, иначе False.
        """
        return await self.topic_router.handle(mqtt_message)
