import datetime as dt
import json
import logging

import orjson
from aiokafka import AIOKafkaProducer

from mqtt_kafka_connector.settings import settings
from mqtt_kafka_connector.utils import DateTimeEncoder, clean_none_fields

logger = logging.getLogger(__name__)


class MessageHelper:
    def __init__(self, prometheus=None):
        self.prometheus = prometheus

    def _check_message_interval(self, msg: dict) -> bool:
        if not (msg_time := msg.get("time")):
            logger.warning("В сообщении нет поля time")
            return False

        if isinstance(msg_time, str):
            msg_time = dt.datetime.fromisoformat(msg_time)

        msg_time = msg_time.astimezone(dt.timezone.utc)
        now_utc = dt.datetime.now(dt.timezone.utc)
        early = now_utc - dt.timedelta(hours=settings.MIN_TELEMETRY_INTERVAL_AGE_HOURS)
        late = now_utc + dt.timedelta(hours=settings.MAX_TELEMETRY_INTERVAL_AGE_HOURS)
        self.prometheus.telemetry_message_lag_add(
            value=(now_utc - msg_time).total_seconds(),
        )

        if not early <= msg_time <= late:
            logger.info("Время сообщения вне допустимого интервала")
            return False
        return True

    def prepare_msg_for_kafka(self, raw_msg: dict) -> bytes | None:
        try:
            if settings.MODIFY_MESSAGE_RM_NONE_FIELDS:
                raw_msg = clean_none_fields(raw_msg)

            # Неявное приведение к стандарту JSON без
            # значений NaN, Inf, -Inf с помощью orjson)
            msg_for_kafka = (
                orjson.dumps(raw_msg)
                if settings.MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS
                else json.dumps(raw_msg, cls=DateTimeEncoder).encode()
            )

            if not self._check_message_interval(msg=raw_msg):
                return None

        except Exception as exc:
            logger.exception("Ошибка при подготовке сообщения для Kafka: %r", exc)
            return None

        return msg_for_kafka


class KafkaProducer:
    def __init__(self, message_helper: MessageHelper):
        self.producer: AIOKafkaProducer = None
        self.message_helper = message_helper

    async def start(self):
        self.producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )
        await self.producer.start()
        logger.info("Продюсер Kafka запущен")

    async def stop(self):
        await self.producer.stop()

    async def get_partition(self, topic: str, key: bytes) -> int:
        partitions = await self.producer.partitions_for(topic)
        return int(key) % len(partitions)

    async def send_batch(
        self,
        topic: str,
        messages: list[dict],
        key: bytes,
        headers: list,
    ):
        batch = self.producer.create_batch()

        i = 0
        while i < len(messages):
            if not (msg := self.message_helper.prepare_msg_for_kafka(messages[i])):
                i += 1
                continue

            metadata = batch.append(key=key, value=msg, timestamp=None, headers=headers)
            if metadata is None:
                partition = await self.get_partition(topic, key)
                fut = await self.producer.send_batch(batch, topic, partition=partition)
                await fut
                logger.info(
                    "Отправлено %s сообщений в батче",
                    batch.record_count(),
                )
                batch = self.producer.create_batch()
                continue
            i += 1

        partition = await self.get_partition(topic, key)
        fut = await self.producer.send_batch(batch, topic, partition=partition)
        await fut
        logger.info(
            "Отправлено %s сообщений в батче",
            batch.record_count(),
        )

    async def send(
        self,
        topic: str,
        message: dict,
        key: bytes,
        headers: list,
    ) -> bool:
        if (value := self.message_helper.prepare_msg_for_kafka(message)) is None:
            return False

        logging.debug(
            "Отправка сообщения в топик %r, ключ %r, заголовки %r, значение %r",
            topic,
            key,
            headers,
            value,
        )
        await self.producer.send_and_wait(
            topic,
            value=value,
            key=key,
            headers=headers,
        )
        return True