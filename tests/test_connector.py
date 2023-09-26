import dataclasses
import datetime
import json
import time
from typing import List
from unittest import mock

import pytest
from aiokafka import errors
from aiomqtt import Message, Topic
from dataclasses_avroschema import AvroModel

from mqtt_kafka_connector.connector import Connector

MQTT_TOPIC = 'customer/11111/dev/22222/v333333'

PAYLOAD = dict(
    messages=[
        dict(
            time=time.time_ns() // 1_000_000,
            speed=45.67,
            lat=12.3456,
            lon=23.4567,
        ),
        dict(
            time=1_600_000_000_111,
            speed=45.68,
            lat=12.3457,
            lon=23.4568,
        ),
    ]
)


def test_get_kafka_producer_params(conn):
    res = conn.get_kafka_producer_params(MQTT_TOPIC)
    assert res is not None

    kafka_topic, kafka_key, kafka_headers = res
    assert kafka_topic == 'telemetry'
    assert kafka_key == b'22222'
    assert set(kafka_headers) == {
        ('schema_id', b'333333'),
    }


@mock.patch('mqtt_kafka_connector.connector.main.AIOKafkaProducer.send')
async def test_send_to_kafka(producer_mock, conn, caplog):
    res = await conn.send_to_kafka(MQTT_TOPIC, value=b'some_bytes1', key=b'1')
    assert res is True
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'INFO'

    conn.producer.send.side_effect = errors.KafkaConnectionError()
    res = await conn.send_to_kafka(
        'unmatched_topic', value=b'some_bytes2', key=b'2'
    )
    assert res is False
    assert len(caplog.records) == 2
    assert caplog.records[0].levelname == 'INFO'


@dataclasses.dataclass
class TestMessage(AvroModel):
    time: datetime.datetime
    speed: float
    lat: float
    lon: float


@dataclasses.dataclass
class TestMessagePack(AvroModel):
    messages: List[TestMessage]


@mock.patch(
    'mqtt_kafka_connector.connector.main.AIOKafkaProducer.start',
    mock.AsyncMock(),
)
@mock.patch(
    'mqtt_kafka_connector.connector.main.AIOKafkaProducer.stop',
    mock.AsyncMock(),
)
@mock.patch('mqtt_kafka_connector.connector.main.schema_client.get_schema')
@pytest.mark.parametrize(
    'message_deserialize,message',
    [
        (True, TestMessagePack(**PAYLOAD).serialize()),
        (False, json.dumps(PAYLOAD).encode()),
    ],
)
async def test_deserialize(get_schema_mock, message_deserialize, message):
    get_schema_mock.return_value = TestMessagePack.avro_schema_to_python()

    msg = Message(
        topic=Topic(MQTT_TOPIC),
        payload=message,
        qos=2,
        retain=False,
        mid=0,
        properties=None,
    )

    conn = Connector(message_deserialize=message_deserialize)
    conn.producer = mock.AsyncMock()
    await conn.mqtt_message_handler(msg)

    assert conn.producer.send.call_count == len(PAYLOAD['messages'])
    call = conn.producer.send.mock_calls[0]
    assert call.args[0] == 'telemetry'
    assert type(call.kwargs['value']) == bytes
    value = json.loads(call.kwargs['value'])
    expected_message = PAYLOAD['messages'][0]
    assert value['time']
    assert value['speed'] == expected_message['speed']
    assert value['lat'] == expected_message['lat']
    assert value['lon'] == expected_message['lon']
    assert type(call.kwargs['key']) == bytes
    assert type(call.kwargs['headers']) == list

    headers = dict(call.kwargs['headers'])
    assert headers['schema_id'] == b'333333'
    if message_deserialize:
        assert headers['message_deserialized'] == b'1'

    assert 'message_uuid' in headers

    for key, value in headers.items():
        assert type(value) == bytes


async def test_serialize_deserialize():
    message = TestMessagePack(**PAYLOAD)
    data_serialized = message.serialize()
    assert isinstance(data_serialized, bytes)

    data_deserialized = TestMessagePack.deserialize(data_serialized).asdict()
    assert isinstance(data_deserialized, dict)
    assert isinstance(
        data_deserialized['messages'][0]['time'], datetime.datetime
    )
