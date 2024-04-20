import datetime


async def test_serialize_deserialize(message_pack):
    data_serialized = message_pack.serialize()
    assert isinstance(data_serialized, bytes)

    data_deserialized = message_pack.deserialize(data_serialized).asdict()
    assert data_deserialized
    assert isinstance(
        data_deserialized['messages'][0]['time'], datetime.datetime
    )
