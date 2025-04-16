from mqtt_kafka_connector import conf
from mqtt_kafka_connector.clients.mqtt import MQTTClient


async def test_client(mqtt_client, message_pack):
    mqtt_client_instance = MQTTClient()
    await mqtt_client_instance.start()
    mqtt_client.assert_called_once_with(
        hostname=conf.MQTT_HOST,
        port=conf.MQTT_PORT,
        username=conf.MQTT_USER,
        password=conf.MQTT_PASSWORD,
        identifier=conf.MQTT_CLIENT_ID,
        clean_session=False,
        timeout=300,
    )

    async for mqtt_message in mqtt_client_instance.get_messages():
        assert mqtt_message.payload == message_pack.serialize()

    mqtt_client_instance.client.subscribe.call_count == 2  # check if the client subscribed to the two topics
