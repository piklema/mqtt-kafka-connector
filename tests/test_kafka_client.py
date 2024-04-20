import asyncio
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mqtt_kafka_connector.clients.kafka import KafkaProducer
from mqtt_kafka_connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS,
    MODIFY_MESSAGE_RM_NONE_FIELDS,
)


@pytest.fixture(scope="module")
def loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def kafka_producer(loop):
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer.create_batch = MagicMock()
    mock_producer.create_batch.return_value.append.side_effect = [
        None,
        '1',
        'metadata',
    ]
    mock_producer.partitions_for = AsyncMock(return_value=[0])
    mock_producer.send_batch = AsyncMock()

    async def send_result():
        res = MagicMock()
        res.partition = 0
        return res

    mock_producer.send = AsyncMock(
        return_value=True, side_effect=[send_result()]
    )

    kafka_producer = KafkaProducer(loop)
    kafka_producer.producer = mock_producer
    return kafka_producer


async def test_send_batch(kafka_producer):
    await kafka_producer.send_batch(
        'topic', 'headers', ['message1', 'message2']
    )

    kafka_producer.producer.create_batch.assert_called()
    kafka_producer.producer.partitions_for.assert_called_with('topic')
    kafka_producer.producer.send_batch.assert_called()


async def test_send(kafka_producer):
    result = await kafka_producer.send(
        'topic', {'message': 'test'}, b'key', [('header', b'value')]
    )

    kafka_producer.producer.send.assert_called_with(
        'topic',
        value=kafka_producer._prepare_msg_for_kafka({'message': 'test'}),
        key=b'key',
        headers=[('header', b'value')],
    )
    assert result is True
