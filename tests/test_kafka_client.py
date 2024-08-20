import datetime as dt

import pytest
from mqtt_kafka_connector.conf import (
    MAX_TELEMETRY_INTERVAL_AGE_HOURS,
    MIN_TELEMETRY_INTERVAL_AGE_HOURS,
)

from .conftest import FAKE_TIME


async def test_send_batch(kafka_producer, unpack_message_pack):
    await kafka_producer.send_batch(
        'topic',
        unpack_message_pack,
        '1',
        [('header_1', 'value')],
    )

    kafka_producer.producer.create_batch.assert_called()
    kafka_producer.producer.partitions_for.assert_called_with('topic')
    kafka_producer.producer.send_batch.assert_called()


async def test_send(kafka_producer, unpack_message_pack, patch_datetime_now):
    result = await kafka_producer.send(
        'topic', unpack_message_pack[0], b'key', [('header', b'value')]
    )

    kafka_producer.producer.send_and_wait.assert_called_with(
        'topic',
        value=kafka_producer.message_helper.prepare_msg_for_kafka(
            unpack_message_pack[0]
        ),
        key=b'key',
        headers=[
            ('header', b'value'),
            ('dt_send_to_kafka', FAKE_TIME.isoformat().encode())
        ]
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
        (
            None,
            False,
        ),
    ],
)
async def test_check_message_interval(kafka_producer, time, expected):
    message = {'time': time}
    assert (
        kafka_producer.message_helper._check_message_interval(message)
        is expected
    )
