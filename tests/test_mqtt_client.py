from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.settings import settings
from unittest import mock


async def test_client(mqtt_client, mock_mqtt_settings):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()

    # Фикстура mqtt_client в conftest.py патчит aiomqtt.Client
    # и возвращает мок, поэтому мы можем проверить, что он был вызван.
    mqtt_client.assert_called_once_with(
        hostname=mock_mqtt_settings.MQTT_HOST,
        port=mock_mqtt_settings.MQTT_PORT,
        username=mock_mqtt_settings.MQTT_USER,
        password=mock_mqtt_settings.MQTT_PASSWORD,
        identifier=mock_mqtt_settings.MQTT_CLIENT_ID,
        clean_session=True,
        timeout=300,
    )


async def test_get_messages(mqtt_client, mock_mqtt_settings):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()

    async for message in mqtt_client_instance.get_messages():
        assert message.topic.value == "topic"
        break

    mqtt_client.return_value.subscribe.assert_any_call(mock_mqtt_settings.MQTT_TOPIC_SOURCE_MATCH, qos=1)
    mqtt_client.return_value.subscribe.assert_any_call(mock_mqtt_settings.MQTT_FSTATE_SOURCE_MATCH, qos=1)

    assert mqtt_client.return_value.subscribe.call_count == 2
