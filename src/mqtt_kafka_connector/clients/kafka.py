import json
import logging

import orjson
from aiokafka import AIOKafkaProducer

from mqtt_kafka_connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS,
    MODIFY_MESSAGE_RM_NONE_FIELDS,
)
from mqtt_kafka_connector.utils import DateTimeEncoder, clean_none_fields

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self):
        self.producer: AIOKafkaProducer = None

    async def start(self):
        self.producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        await self.producer.start()
        logger.info('Kafka Producer is running')

    async def stop(self):
        await self.producer.stop()

    @staticmethod
    def _prepare_msg_for_kafka(raw_msg: dict) -> bytes:
        if MODIFY_MESSAGE_RM_NONE_FIELDS:
            raw_msg = clean_none_fields(raw_msg)

        # Implicit casting to JSON standard without
        # NaN, Inf, -Inf values with orjson)
        msg_for_kafka = (
            orjson.dumps(raw_msg)
            if MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS
            else json.dumps(raw_msg, cls=DateTimeEncoder).encode()
        )
        return msg_for_kafka

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
            msg = self._prepare_msg_for_kafka(messages[i])
            metadata = batch.append(
                key=key, value=msg, timestamp=None, headers=headers
            )
            if metadata is None:
                partition = await self.get_partition(topic, key)
                fut = await self.producer.send_batch(
                    batch, topic, partition=partition
                )
                res = await fut
                logger.info(
                    'Sent batch %s messages sent to partition %s',
                    batch.record_count(),
                    res.partition,
                )
                batch = self.producer.create_batch()
                continue
            i += 1

        partition = await self.get_partition(topic, key)
        fut = await self.producer.send_batch(batch, topic, partition=partition)
        res = await fut
        logger.info(
            'Sent batch %s messages to partition %s',
            batch.record_count(),
            res.partition,
        )

    async def send(
        self,
        topic: str,
        message: dict,
        key: bytes,
        headers: list,
    ) -> bool:
        value = self._prepare_msg_for_kafka(message)
        res = await self.producer.send_and_wait(
            topic, value=value, key=key, headers=headers
        )
        logger.info(
            '1 message sent with key %s to partition %s', key, res.partition
        )
        return True
