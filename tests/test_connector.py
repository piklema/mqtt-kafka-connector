from unittest import mock

import pytest
from aiokafka import errors
from asyncio_mqtt import Message, Topic

from connector.main import mqtt_message_handler, send_to_kafka

pytestmark = pytest.mark.asyncio


@mock.patch('connector.main.AIOKafkaProducer.send')
async def test_send_to_kafka(producer_mock, caplog):
    with mock.patch(
        'connector.main.AIOKafkaProducer.start', new_callable=mock.AsyncMock
    ):
        res = await send_to_kafka('customer_1', value=b'some_bytes1')
        assert res is True
        assert len(caplog.records) == 1
        assert caplog.records[-1].levelname == 'INFO'

    with mock.patch(
        'connector.main.AIOKafkaProducer.start', new_callable=mock.AsyncMock
    ) as start_mock:

        start_mock.side_effect = errors.KafkaConnectionError()
        res = await send_to_kafka('customer_2', value=b'some_bytes2')
        assert res is False
        assert len(caplog.records) == 2
        assert caplog.records[-1].levelname == 'ERROR'


@mock.patch('connector.main.send_to_kafka')
async def test_mqtt_handler(send_to_kafka_mock, caplog):
    send_to_kafka_mock.return_value = True

    topic = Topic('customer/1/dev/22')
    msg = Message(
        topic=topic, payload=b'some_payload', qos=2, retain=False, mid=0
    )

    res = await mqtt_message_handler(msg)
    assert res is True
    assert send_to_kafka_mock.call_args.args[0] == 'customer_1'
    assert send_to_kafka_mock.call_args.args[1] == b'some_payload'
    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == 'INFO'

    msg.topic = Topic('bad_topic')
    res = await mqtt_message_handler(msg)
    assert res is False

    assert len(caplog.records) == 3
    assert caplog.records[-1].levelname == 'WARNING'
