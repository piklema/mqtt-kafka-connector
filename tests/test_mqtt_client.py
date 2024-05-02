from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.conf import (
    MQTT_CLIENT_ID,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_TOPIC_SOURCE_MATCH,
    MQTT_USER,
)


async def test_client(mqtt_client, message_pack):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()
    mqtt_client.assert_called_once_with(
        hostname=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASSWORD,
        identifier=MQTT_CLIENT_ID,
        clean_session=False,
        timeout=300,
    )

    async for mqtt_message in mqtt_client_instance.get_messages():
        assert mqtt_message == message_pack.serialize()

    mqtt_client_instance.client.subscribe.assert_called_once_with(
        MQTT_TOPIC_SOURCE_MATCH, qos=1
    )
