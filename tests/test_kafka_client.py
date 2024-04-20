

async def test_send_batch(kafka_producer):
    await kafka_producer.send_batch(
        'topic', 'headers', ['message1', 'message2']
    )

    kafka_producer.producer.create_batch.assert_called()
    kafka_producer.producer.partitions_for.assert_called_with('topic')
    kafka_producer.producer.send_batch.assert_called()


async def test_send(kafka_producer):
    result = await kafka_producer.send(
        'topic', {'message': 'test'}, b'key', [('header', b'value')]
    )

    kafka_producer.producer.send.assert_called_with(
        'topic',
        value=kafka_producer._prepare_msg_for_kafka({'message': 'test'}),
        key=b'key',
        headers=[('header', b'value')],
    )
    assert result is True
