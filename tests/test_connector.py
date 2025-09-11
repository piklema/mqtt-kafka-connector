from unittest import mock

import pytest
from aiomqtt.message import Message

from mqtt_kafka_connector.connector.connector import Connector
from tests.conftest import create_mqtt_message


@pytest.fixture
def topic_router():
    return mock.AsyncMock()


@pytest.fixture
def connector(mqtt_client, kafka_producer, topic_router, prometheus):
    return Connector(mqtt_client, kafka_producer, topic_router, prometheus)


async def test_connector_handle(connector, topic_router):
    message = create_mqtt_message("some/topic")
    await connector.handle(message)
    topic_router.handle.assert_called_once_with(message)