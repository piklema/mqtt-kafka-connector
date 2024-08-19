import datetime as dt
import json
from unittest import mock

from aiomqtt.message import Message
from mqtt_kafka_connector.connector.main import Connector
from mqtt_kafka_connector.context_vars import customer_id_var, device_id_var
from zoneinfo import ZoneInfo

TZ = ZoneInfo('UTC')
DEVICE_ID = '22222'
SCHEMA_ID = '333333'
CUSTOMER_ID = '11111'
MQTT_TOPIC = f'customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}'


@mock.patch('httpx.AsyncClient.request')
async def test_connector(
    schema_client,
    mqtt_client,
    kafka_producer,
    message_pack,
    schema,
    prometheus,
):
    connector = Connector(
        mqtt_client, kafka_producer, schema_client, prometheus
    )
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
    assert customer_id_var.get() == CUSTOMER_ID
    assert device_id_var.get() == DEVICE_ID

    send_batch_kwargs = (
        connector.kafka_producer.producer.create_batch.return_value.mock_calls[
            0
        ].kwargs
    )
    assert send_batch_kwargs['key'] == DEVICE_ID.encode()

    message = message_pack.messages[0]
    message['time'] = dt.datetime.fromtimestamp(
        message['time'] / 1000, TZ
    ).isoformat()

    sending_message = json.loads(send_batch_kwargs['value'])
    assert sending_message == message

    headers = dict(send_batch_kwargs['headers'])
    assert headers.pop('message_uuid')
    assert headers == dict(
        schema_id=SCHEMA_ID.encode(),
        message_deserialized=b'1',
    )
