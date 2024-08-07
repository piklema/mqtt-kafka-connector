import datetime as dt

import pytest
from mqtt_kafka_connector.conf import (
    MAX_TELEMETRY_INTERVAL_AGE_HOURS,
    MIN_TELEMETRY_INTERVAL_AGE_HOURS,
)


async def test_send_batch(kafka_producer, unpack_message_pack):
    await kafka_producer.send_batch(
        'topic',
        unpack_message_pack,
        '1',
        'headers',
    )

    kafka_producer.producer.create_batch.assert_called()
    kafka_producer.producer.partitions_for.assert_called_with('topic')
    kafka_producer.producer.send_batch.assert_called()


async def test_send(kafka_producer, message_pack):
    result = await kafka_producer.send(
        'topic', message_pack, b'key', [('header', b'value')]
    )

    kafka_producer.producer.send_and_wait.assert_called_with(
        'topic',
        value=kafka_producer._prepare_msg_for_kafka(message_pack),
        key=b'key',
        headers=[('header', b'value')],
    )
    assert result is True


@pytest.mark.parametrize(
    'time,expected',
    [
        (dt.datetime.now(), True),
        (
            dt.datetime.now()
            - dt.timedelta(hours=MIN_TELEMETRY_INTERVAL_AGE_HOURS),
            False,
        ),
        (
            dt.datetime.now()
            + dt.timedelta(hours=1, minutes=MAX_TELEMETRY_INTERVAL_AGE_HOURS),
            False,
        ),
    ],
)
async def test_check_message_interval(kafka_producer, time, expected):
    message = {'time': time}
    assert kafka_producer._check_message_interval(message) is expected
