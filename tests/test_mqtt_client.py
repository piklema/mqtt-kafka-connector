from unittest import mock

import pytest

from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.conf import (
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)


@pytest.fixture
async def mock_mqtt_client(monkeypatch):
    mock_message_data = 'mock_msg'
    mock_client = mock.MagicMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.messages.__aiter__.return_value = iter([mock_message_data])
    mock_client.subscribe = mock.AsyncMock()

    mqtt_client = mock.MagicMock(return_value=mock_client)
    monkeypatch.setattr("aiomqtt.Client", mqtt_client)
    return mqtt_client


async def test_client(mock_mqtt_client):
    mqtt_client = MQTTClient()

    mock_mqtt_client.assert_called_once_with(
        hostname=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASSWORD,
        identifier=MQTT_CLIENT_ID,
        clean_session=False,
        timeout=300,
    )

    async for mqtt_message in mqtt_client.get_messages():
        assert mqtt_message == 'mock_msg'

    mqtt_client.client.subscribe.assert_called_once_with(
        MQTT_TOPIC_SOURCE_MATCH, qos=1
    )
