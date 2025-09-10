import datetime as dt
import json
from unittest import mock

import orjson
import pytest
from aiomqtt.message import Message
from zoneinfo import ZoneInfo

from mqtt_kafka_connector.connector.handlers import (
    FStateHandler,
    TelemetryHandler,
    TopicRouter,
)
from mqtt_kafka_connector.context_vars import customer_id_var, device_id_var

TZ = ZoneInfo("UTC")
DEVICE_ID = "22222"
SCHEMA_ID = "333333"
CUSTOMER_ID = "11111"
MQTT_TOPIC = f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}"
MQTT_FSTATE_TOPIC = f"fstate/{CUSTOMER_ID}/truck/{DEVICE_ID}"


@pytest.fixture
def telemetry_handler_mock():
    return mock.AsyncMock(spec=TelemetryHandler)


@pytest.fixture
def fstate_handler_mock():
    return mock.AsyncMock(spec=FStateHandler)


@pytest.fixture
def kafka_producer():
    return mock.AsyncMock()


@pytest.fixture
def schema_client(schema):
    client = mock.AsyncMock()
    client.get_schema.return_value = schema
    return client


@pytest.fixture
def prometheus():
    return mock.AsyncMock()


@pytest.fixture
def fstate_handler(kafka_producer):
    return FStateHandler(kafka_producer)


@pytest.fixture
def telemetry_handler(kafka_producer, schema_client, prometheus):
    return TelemetryHandler(kafka_producer, schema_client, prometheus)


@pytest.fixture
def topic_router(telemetry_handler_mock, fstate_handler_mock):
    return TopicRouter(
        telemetry_handler=telemetry_handler_mock,
        fstate_handler=fstate_handler_mock,
    )


def _get_message(topic: str, payload: bytes = b"test_payload") -> Message:
    return Message(
        topic=topic,
        payload=payload,
        qos=1,
        retain=False,
        mid=1,
        properties=None,
    )


async def test_topic_router_telemetry(
    topic_router, telemetry_handler_mock, fstate_handler_mock
):
    message = _get_message(f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}")
    await topic_router.handle(message)

    telemetry_handler_mock.handle.assert_called_once_with(message)
    fstate_handler_mock.handle.assert_not_called()


async def test_topic_router_fstate(
    topic_router, telemetry_handler_mock, fstate_handler_mock
):
    message = _get_message(f"fstate/{CUSTOMER_ID}/truck/{DEVICE_ID}")
    await topic_router.handle(message)

    fstate_handler_mock.handle.assert_called_once_with(message)
    telemetry_handler_mock.handle.assert_not_called()


async def test_topic_router_unknown(
    topic_router, telemetry_handler_mock, fstate_handler_mock, caplog
):
    message = _get_message("unknown/topic")
    await topic_router.handle(message)

    fstate_handler_mock.handle.assert_not_called()
    telemetry_handler_mock.handle.assert_not_called()
    assert "Неизвестный топик" in caplog.text


async def test_telemetry_handler(telemetry_handler, message_pack):
    message = _get_message(
        topic=MQTT_TOPIC,
        payload=message_pack.serialize(),
    )
    res = await telemetry_handler.handle(message)
    assert res is True
    assert customer_id_var.get() == CUSTOMER_ID
    assert device_id_var.get() == DEVICE_ID

    send_batch_args = (
        telemetry_handler.kafka_producer.send_batch.call_args.args
    )
    assert send_batch_args[2] == DEVICE_ID.encode()

    sent_message = send_batch_args[1][0]
    unpacked_message = message_pack.messages[0]

    assert int(sent_message["time"].timestamp() * 1000) == unpacked_message["time"]

    headers = dict(send_batch_args[3])
    assert headers.pop("message_uuid")
    assert headers == dict(
        schema_id=SCHEMA_ID.encode(),
        message_deserialized=b"1",
    )


async def test_fstate_handler(fstate_handler):
    now = dt.datetime.now().isoformat()
    payload = f'{{"time": "{now}"}}'.encode()
    message = _get_message(
        topic=MQTT_FSTATE_TOPIC,
        payload=payload,
    )
    res = await fstate_handler.handle(message)
    assert res is True
    assert customer_id_var.get() == CUSTOMER_ID
    assert device_id_var.get() == DEVICE_ID

    assert fstate_handler.kafka_producer.send.call_count == 1
    send_call_args = fstate_handler.kafka_producer.send.call_args
    assert send_call_args.args[0] == "fstate"
    assert send_call_args.kwargs["message"] == orjson.loads(payload)


async def test_fstate_handler_not_valid_json(fstate_handler, caplog):
    payload = b"not valid json"
    message = _get_message(
        topic=MQTT_FSTATE_TOPIC,
        payload=payload,
    )
    res = await fstate_handler.handle(message)
    assert res is False
    assert "Ошибка декодирования JSON" in caplog.text


async def test_fstate_handler_with_bad_topic(fstate_handler, caplog):
    payload = b"{}"
    message = _get_message(
        topic=f"fstate/{CUSTOMER_ID}/truck/",
        payload=payload,
    )
    res = await fstate_handler.handle(message)
    assert res is False
    assert "не найден ID устройства" in caplog.text
