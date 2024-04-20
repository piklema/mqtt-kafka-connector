from unittest import mock

import pytest

from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.connector import Connector
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


@pytest.fixture
async def mqtt_client(monkeypatch):
    mock_message_data = 'mock_msg'
    mock_client = mock.MagicMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.messages.__aiter__.return_value = iter([mock_message_data])
    mock_client.subscribe = mock.AsyncMock()

    mqtt_client = mock.MagicMock(return_value=mock_client)
    monkeypatch.setattr("aiomqtt.Client", mqtt_client)
    return mqtt_client


@pytest.fixture
def conn():
    conn = Connector()
    conn.kafka_producer = mock.AsyncMock()

    return conn


class DummyResponse:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data

    def raise_for_status(self):
        pass
