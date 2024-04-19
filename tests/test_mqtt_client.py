from unittest.mock import AsyncMock, patch
from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.conf import (
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)


@patch('mqtt_kafka_connector.clients.mqtt.aiomqtt.Client')
def test_mqtt_client_init(mock_client):
    MQTTClient()
    mock_client.assert_called_once_with(
        hostname=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASSWORD,
        identifier=MQTT_CLIENT_ID,
        clean_session=False,
        timeout=300,
    )


@patch('mqtt_kafka_connector.clients.mqtt.aiomqtt.Client')
async def test_get_messages(mock_client):

    mock_message_data = 'mock_msg'

    mock_client_instance = mock_client.return_value
    mock_client_instance.__aenter__.return_value = mock_client_instance
    mock_client_instance.messages.__aiter__.return_value = iter([mock_message_data])
    mock_client_instance.subscribe = AsyncMock()

    mqtt_client = MQTTClient()
    async for mqtt_message in mqtt_client.get_messages():
        assert mqtt_message == mock_message_data

    mock_client_instance.subscribe.assert_called_once_with(MQTT_TOPIC_SOURCE_MATCH, qos=1)
