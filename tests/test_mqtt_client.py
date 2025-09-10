from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.settings import settings


async def test_client(mqtt_client):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()
    # Фикстура mqtt_client в conftest.py патчит aiomqtt.Client
    # и возвращает мок, поэтому мы можем проверить, что он был вызван.
    mqtt_client.assert_called_once_with(
        hostname=settings.MQTT_HOST,
        port=settings.MQTT_PORT,
        username=settings.MQTT_USER,
        password=settings.MQTT_PASSWORD,
        identifier=settings.MQTT_CLIENT_ID,
        clean_session=False,
        timeout=300,
    )


async def test_get_messages(mqtt_client):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()

    async for message in mqtt_client_instance.get_messages():
        assert message.topic.value == "topic"
        break

    # Клиент - это MagicMock, поэтому мы можем получить доступ к атрибуту subscribe,
    # который является AsyncMock, и проверить, что он был вызван.
    mqtt_client.return_value.subscribe.assert_any_call(settings.MQTT_TOPIC_SOURCE_MATCH, qos=1)
    mqtt_client.return_value.subscribe.assert_any_call(settings.MQTT_FSTATE_SOURCE_MATCH, qos=1)

    assert mqtt_client.return_value.subscribe.call_count == 2