import datetime as dt
import json
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from aiomqtt.message import Message

from mqtt_kafka_connector.connector.main import Connector
from mqtt_kafka_connector.context_vars import customer_id_var, device_id_var

TZ = ZoneInfo("UTC")
DEVICE_ID = "22222"
SCHEMA_ID = "333333"
CUSTOMER_ID = "11111"
MQTT_TOPIC = f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}"
MQTT_FSTATE_TOPIC = f"fstate/{CUSTOMER_ID}/truck/{DEVICE_ID}"


@pytest.fixture
@mock.patch("httpx.AsyncClient.request")
def connector(schema_client, mqtt_client, kafka_producer, prometheus, schema):
    schema_client.get_schema.return_value = schema
    return Connector(mqtt_client, kafka_producer, schema_client, prometheus)


def _Message(topic, payload):
    return Message(
        topic=topic,
        payload=payload,
        qos=1,
        retain=False,
        mid=1,
        properties=None,
    )


async def test_connector_telemetry_message(connector, message_pack):
    message = _Message(
        topic=MQTT_TOPIC,
        payload=message_pack.serialize(),
    )
    res = await connector.handle(message)
    assert res is True
    assert customer_id_var.get() == CUSTOMER_ID
    assert device_id_var.get() == DEVICE_ID

    send_batch_kwargs = (
        connector.kafka_producer.producer.create_batch.return_value.mock_calls[0].kwargs
    )
    assert send_batch_kwargs["key"] == DEVICE_ID.encode()

    message = message_pack.messages[0]
    message["time"] = dt.datetime.fromtimestamp(message["time"] / 1000, TZ).isoformat()

    sending_message = json.loads(send_batch_kwargs["value"])
    assert sending_message == message

    headers = dict(send_batch_kwargs["headers"])
    assert headers.pop("message_uuid")
    assert headers == dict(
        schema_id=SCHEMA_ID.encode(),
        message_deserialized=b"1",
    )


async def test_connector_fstate_message(connector):
    now = dt.datetime.now().isoformat()
    payload = f'{{"time": "{now}"}}'.encode()
    message = _Message(
        topic=MQTT_FSTATE_TOPIC,
        payload=payload,
    )
    res = await connector.handle(message)
    assert res is True
    assert customer_id_var.get() == CUSTOMER_ID
    assert device_id_var.get() == DEVICE_ID

    assert connector.kafka_producer.producer.send_and_wait.call_count == 1
    send_and_wait_call_args = connector.kafka_producer.producer.send_and_wait.call_args
    assert send_and_wait_call_args.args[0] == "fstate"
    assert send_and_wait_call_args.kwargs["value"] == payload


async def test_connector_fstate_message_not_valid_json(connector):
    payload = "not valid json"
    message = _Message(
        topic="fstate/{CUSTOMER_ID}/shovel/{DEVICE_ID}",
        payload=payload,
    )
    res = await connector.handle(message)
    assert res is False


async def test_connector_fstate_message_with_bad_topic(connector):
    payload = "{}"
    message = _Message(
        topic="fstate/{CUSTOMER_ID}/shovel/{DEVICE_ID}",
        payload=payload,
    )
    res = await connector.handle(message)
    assert res is False
