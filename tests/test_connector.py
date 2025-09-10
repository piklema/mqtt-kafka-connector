from unittest import mock

import pytest
from aiomqtt.message import Message

from mqtt_kafka_connector.connector.connector import Connector


@pytest.fixture
def topic_router():
    return mock.AsyncMock()


@pytest.fixture
def connector(mqtt_client, kafka_producer, topic_router, prometheus):
    return Connector(mqtt_client, kafka_producer, topic_router, prometheus)


def _get_message(topic: str, payload: bytes = b"test_payload") -> Message:
    return Message(
        topic=topic,
        payload=payload,
        qos=1,
        retain=False,
        mid=1,
        properties=None,
    )


async def test_connector_handle(connector, topic_router):
    message = _get_message("some/topic")
    await connector.handle(message)
    topic_router.handle.assert_called_once_with(message)