from unittest import mock

from aiomqtt.message import Message
from mqtt_kafka_connector.connector.main import Connector

MQTT_TOPIC = 'customer/11111/dev/22222/v333333'


@mock.patch('httpx.AsyncClient.request')
async def test_connector(
    schema_client, mqtt_client, kafka_producer, message_pack, schema
):
    connector = Connector(mqtt_client, kafka_producer, schema_client)
    schema_client.get_schema.return_value = schema
    message = Message(
        topic=MQTT_TOPIC,
        payload=message_pack.serialize(),
        qos=1,
        retain=False,
        mid=1,
        properties=None,
    )
    res = await connector.handle(message)
    assert res is True
