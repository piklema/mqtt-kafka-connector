import dataclasses
from unittest import mock

from aiokafka import errors
from asyncio_mqtt import Message, Topic
from dataclasses_avroschema import AvroModel
import json
from mqtt_kafka_connector.connector import Connector

MQTT_TOPIC = 'customer/11111/dev/22222/v333333'

PAYLOAD = dict(
    messages=[
        dict(
            time=1234567890,
            speed=45.67,
            lat=12.3456,
            lon=23.4567,
        ),
        dict(
            time=1234567891,
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
    with mock.patch(
        'mqtt_kafka_connector.connector.main.AIOKafkaProducer.start',
        new_callable=mock.AsyncMock,
    ):
        res = await conn.send_to_kafka(
            MQTT_TOPIC, value=b'some_bytes1', key=b'1'
        )
        assert res is True
        assert len(caplog.records) == 1
        assert caplog.records[-1].levelname == 'INFO'

    with mock.patch(
        'mqtt_kafka_connector.connector.main.AIOKafkaProducer.start',
        new_callable=mock.AsyncMock,
    ) as start_mock:
        start_mock.side_effect = errors.KafkaConnectionError()
        res = await conn.send_to_kafka(
            'unmatched_topic', value=b'some_bytes2', key=b'2'
        )
        assert res is False
        assert len(caplog.records) == 2
        assert caplog.records[-1].levelname == 'ERROR'


async def test_mqtt_handler(conn, caplog):
    mqtt_topic = Topic(MQTT_TOPIC)
    msg = Message(
        topic=mqtt_topic,
        payload=json.dumps(PAYLOAD).encode(),
        qos=2,
        retain=False,
        mid=0,
        properties=None,
    )

    send_to_kafka_mock = mock.AsyncMock()
    conn.send_to_kafka = send_to_kafka_mock
    send_to_kafka_mock.return_value = True

    res = await conn.mqtt_message_handler(msg)
    assert res is True

    assert send_to_kafka_mock.call_count == len(PAYLOAD['messages'])

    call = send_to_kafka_mock.mock_calls[0]
    assert call.args[0] == 'telemetry'
    assert type(call.kwargs['value']) == bytes
    value = json.loads(call.kwargs['value'])
    assert value['time'] == PAYLOAD['messages'][0]['time']
    assert value['speed'] == PAYLOAD['messages'][0]['speed']
    assert value['lat'] == PAYLOAD['messages'][0]['lat']
    assert value['lon'] == PAYLOAD['messages'][0]['lon']
    assert type(call.kwargs['key']) == bytes
    assert type(call.kwargs['headers']) == list

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == 'INFO'

    msg.topic = Topic('bad_topic')
    res = await conn.mqtt_message_handler(msg)
    assert res is False

    assert len(caplog.records) == 3
    assert caplog.records[-1].levelname == 'WARNING'


@dataclasses.dataclass
class TestMessage(AvroModel):
    time: float
    speed: float
    lat: float
    lon: float


@dataclasses.dataclass
class TestMessagePack(AvroModel):
    messages: list[TestMessage]


@mock.patch(
    'mqtt_kafka_connector.connector.main.AIOKafkaProducer.start',
    mock.AsyncMock(),
)
@mock.patch(
    'mqtt_kafka_connector.connector.main.AIOKafkaProducer.stop',
    mock.AsyncMock(),
)
@mock.patch('mqtt_kafka_connector.connector.main.AIOKafkaProducer.send')
@mock.patch('mqtt_kafka_connector.connector.main.schema_client.get_schema')
async def test_deserialize(schema_mock, kafka_mock):
    topic = Topic(MQTT_TOPIC)
    schema_mock.return_value = TestMessagePack.avro_schema_to_python()

    conn = Connector(message_deserialize=True)

    message = TestMessagePack(**PAYLOAD)
    msg = Message(
        topic=topic,
        payload=message.serialize(),
        qos=2,
        retain=False,
        mid=0,
        properties=None,
    )
    await conn.mqtt_message_handler(msg)

    assert kafka_mock.call_count == len(PAYLOAD['messages'])

    call = kafka_mock.mock_calls[0]
    assert call.args[0] == 'telemetry'
    assert type(call.kwargs['value']) == bytes
    value = json.loads(call.kwargs['value'])
    assert value['time'] == PAYLOAD['messages'][0]['time']
    assert value['speed'] == PAYLOAD['messages'][0]['speed']
    assert value['lat'] == PAYLOAD['messages'][0]['lat']
    assert value['lon'] == PAYLOAD['messages'][0]['lon']
    assert type(call.kwargs['key']) == bytes
    assert type(call.kwargs['headers']) == list

    headers = dict(call.kwargs['headers'])
    assert headers['schema_id'] == b'333333'
    assert 'message_uuid' in headers
