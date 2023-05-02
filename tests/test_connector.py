import dataclasses
from unittest import mock

from aiokafka import errors
from asyncio_mqtt import Message, Topic
from dataclasses_avroschema import AvroModel

from mqtt_kafka_connector.connector import Connector

MQTT_TOPIC = 'customer/11111/dev/22222/v333333'


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
    topic = Topic(MQTT_TOPIC)
    msg = Message(
        topic=topic,
        payload=b'some_payload',
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

    assert send_to_kafka_mock.call_args.args[0] == 'telemetry'
    assert send_to_kafka_mock.call_args.args[1] == b'some_payload'
    assert 'headers' in send_to_kafka_mock.call_args.kwargs
    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == 'INFO'

    msg.topic = Topic('bad_topic')
    res = await conn.mqtt_message_handler(msg)
    assert res is False

    assert len(caplog.records) == 3
    assert caplog.records[-1].levelname == 'WARNING'


@dataclasses.dataclass
class TestMessage(AvroModel):
    test_tag: float


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
    schema_mock.return_value = TestMessage.avro_schema_to_python()

    conn = Connector(message_deserialize=True)

    message = TestMessage(test_tag=11.01)
    msg = Message(
        topic=topic,
        payload=message.serialize(),
        qos=2,
        retain=False,
        mid=0,
        properties=None,
    )
    await conn.mqtt_message_handler(msg)

    assert kafka_mock.call_count == 1
    assert kafka_mock.call_args[0][0] == 'telemetry'
    assert kafka_mock.call_args[0][1] == b'{"test_tag": 11.01}'
    assert kafka_mock.call_args.kwargs['key'] == b'22222'
    headers = dict(kafka_mock.call_args.kwargs['headers'])
    assert headers['schema_id'] == b'333333'
    assert 'message_uuid' in headers
