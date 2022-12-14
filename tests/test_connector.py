from unittest import mock

import pytest
from aiokafka import errors
from asyncio_mqtt import Message, Topic

MQTT_TOPIC = 'customer/CUSTOMER_ID/dev/DEVICE_ID/v42'


def test_get_kafka_producer_params(conn):
    res = conn.get_kafka_producer_params(MQTT_TOPIC)
    assert res is not None

    kafka_topic, kafka_headers = res
    assert kafka_topic == 'customer_CUSTOMER_ID'
    assert set(kafka_headers) == {
        ('schema_version', b'42'),
        ('device_id', b'DEVICE_ID'),
    }


@pytest.mark.asyncio
@mock.patch('connector.main.AIOKafkaProducer.send')
async def test_send_to_kafka(producer_mock, conn, caplog):
    with mock.patch(
        'connector.main.AIOKafkaProducer.start', new_callable=mock.AsyncMock
    ):
        res = await conn.send_to_kafka(MQTT_TOPIC, value=b'some_bytes1')
        assert res is True
        assert len(caplog.records) == 1
        assert caplog.records[-1].levelname == 'INFO'

    with mock.patch(
        'connector.main.AIOKafkaProducer.start', new_callable=mock.AsyncMock
    ) as start_mock:

        start_mock.side_effect = errors.KafkaConnectionError()
        res = await conn.send_to_kafka('unmatched_topic', value=b'some_bytes2')
        assert res is False
        assert len(caplog.records) == 2
        assert caplog.records[-1].levelname == 'ERROR'


@pytest.mark.asyncio
# @mock.patch('connector.main.send_to_kafka')
async def test_mqtt_handler(conn, caplog):

    topic = Topic(MQTT_TOPIC)
    msg = Message(
        topic=topic, payload=b'some_payload', qos=2, retain=False, mid=0
    )

    send_to_kafka_mock = mock.AsyncMock()
    conn.send_to_kafka = send_to_kafka_mock
    send_to_kafka_mock.return_value = True

    res = await conn.mqtt_message_handler(msg)
    assert res is True

    assert send_to_kafka_mock.call_args.args[0] == 'customer_CUSTOMER_ID'
    assert send_to_kafka_mock.call_args.args[1] == b'some_payload'
    assert 'headers' in send_to_kafka_mock.call_args.kwargs
    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == 'INFO'

    msg.topic = Topic('bad_topic')
    res = await conn.mqtt_message_handler(msg)
    assert res is False

    assert len(caplog.records) == 3
    assert caplog.records[-1].levelname == 'WARNING'
