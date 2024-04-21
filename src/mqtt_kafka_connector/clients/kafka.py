import json
import logging
import random

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
    def __init__(self, loop):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, loop=loop
        )

    async def start(self):
        await self.producer.start()

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

    async def send_batch(self, topic, headers, messages: list):
        batch = self.producer.create_batch()

        i = 0
        while i < len(messages):
            msg = self._prepare_msg_for_kafka(messages[i])
            metadata = batch.append(
                key=None, value=msg, timestamp=None, headers=headers
            )
            if metadata is None:
                partitions = await self.producer.partitions_for(topic)
                partition = random.choice(tuple(partitions))
                await self.producer.send_batch(
                    batch, topic, partition=partition
                )
                logger.info(
                    '%s messages sent to partition %s',
                    batch.record_count(),
                    partition,
                )
                batch = self.producer.create_batch()
                continue
            i += 1

        partitions = await self.producer.partitions_for(topic)
        partition = random.choice(tuple(partitions))
        await self.producer.send_batch(batch, topic, partition=partition)
        logger.info(
            '%s messages sent to partition %s', batch.record_count(), partition
        )

    async def send(
        self,
        topic: str,
        message: dict,
        key: bytes,
        headers: list,
    ) -> bool:
        value = self._prepare_msg_for_kafka(message)
        fut = await self.producer.send(
            topic, value=value, key=key, headers=headers
        )
        res = await fut
        logger.info('1 message sent to partition %s', res.partition)
        return True
