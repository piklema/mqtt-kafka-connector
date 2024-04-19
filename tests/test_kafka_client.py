from unittest.mock import AsyncMock, patch, MagicMock
from mqtt_kafka_connector.clients.kafka import KafkaProducer
from mqtt_kafka_connector.conf import (
    KAFKA_BOOTSTRAP_SERVERS,
    MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS,
    MODIFY_MESSAGE_RM_NONE_FIELDS,
)


@patch('mqtt_kafka_connector.clients.kafka.AIOKafkaProducer')
def test_kafka_producer_init(mock_producer):
    KafkaProducer('loop')
    mock_producer.assert_called_once_with(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, loop='loop'
    )


@patch('mqtt_kafka_connector.clients.kafka.AIOKafkaProducer')
async def test_start(mock_producer):
    mock_producer_instance = mock_producer.return_value
    mock_producer_instance.start = AsyncMock()

    kafka_producer = KafkaProducer('loop')
    await kafka_producer.start()

    mock_producer_instance.start.assert_called_once()


@patch('mqtt_kafka_connector.clients.kafka.AIOKafkaProducer')
async def test_stop(mock_producer):
    mock_producer_instance = mock_producer.return_value
    mock_producer_instance.stop = AsyncMock()

    kafka_producer = KafkaProducer('loop')
    await kafka_producer.stop()

    mock_producer_instance.stop.assert_called_once()


@patch('mqtt_kafka_connector.clients.kafka.AIOKafkaProducer')
async def test_send_batch(mock_producer):
    mock_producer_instance = mock_producer.return_value

    mock_producer_instance.create_batch = MagicMock()
    mock_producer_instance.create_batch.return_value.append.side_effect = [None, '1', 'metadata' ]
    mock_producer_instance.partitions_for = AsyncMock(return_value=[0])
    mock_producer_instance.send_batch = AsyncMock()

    kafka_producer = KafkaProducer('loop')
    await kafka_producer.send_batch('topic', 'headers', ['message1', 'message2'])

    mock_producer_instance.create_batch.assert_called()
    mock_producer_instance.partitions_for.assert_called_with('topic')
    mock_producer_instance.send_batch.assert_called()


@patch('mqtt_kafka_connector.clients.kafka.AIOKafkaProducer')
async def test_send(mock_producer):
    mock_producer_instance = mock_producer.return_value

    async def send_result():
        res = MagicMock()
        res.partition = 10
        return res

    mock_producer_instance.send = AsyncMock(return_value=True, side_effect=[send_result()])

    kafka_producer = KafkaProducer('loop')
    result = await kafka_producer.send('topic', {'message': 'test'}, b'key', [('header', b'value')])

    mock_producer_instance.send.assert_called_with('topic', value=kafka_producer._prepare_msg_for_kafka({'message': 'test'}), key=b'key', headers=[('header', b'value')])
    assert result is True
