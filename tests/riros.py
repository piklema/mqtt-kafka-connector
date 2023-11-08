import dataclasses
import datetime
import json

from dataclasses_avroschema import AvroModel, ModelGenerator
from deepdiff import DeepDiff
from orjson import orjson

from tests.test_connector import MessagePack


async def test_serialize_deserialize_bad_data():
    """https://piklema.atlassian.net/browse/NGUK-520"""
    import sys

    PAYLOAD_BAD = dict(
        messages=[
            dict(
                time=datetime.datetime.now(tz=datetime.UTC).replace(
                    microsecond=0,
                ),  # Avro обрезает миллисекунды '2023-12-06T11:28:54.862445+00:00' -> '2023-12-06T11:28:54.862000+00:00'
                speed=sys.float_info.max,
                lat=-33.333,
                lon=0.0,  # lon=float(o'nan'),  # Ошибка сравнения в DeepDiff
            ),
        ],
    )
    message_pack = MessagePack(**PAYLOAD_BAD)
    assert message_pack.validate()

    message_pack_dict = message_pack.to_dict()
    data_serialized = message_pack.serialize()

    data_deserialized = MessagePack.deserialize(data_serialized)
    data_deserialized_dict = data_deserialized.asdict()
    assert not DeepDiff(message_pack_dict, data_deserialized_dict)


async def test_serialize_deserialize_nan_to_none():
    """https://piklema.atlassian.net/browse/NGUK-520"""
    bson = b'{ "speed": NaN }'
    a_json = json.loads(bson.decode())
    a_json_dump = json.dumps(a_json, separators=(",", ":")).encode()
    a_orjson_dump = orjson.dumps(a_json)
    assert a_orjson_dump != a_json_dump  # NaN -> none


async def test_avro_schema_to_python_code_generation():
    @dataclasses.dataclass
    class TestModel(AvroModel):
        data: str
        optional_data: str | None = None

    test_model = TestModel(data="data")  # не указали optional_data
    assert test_model

    model_generator = ModelGenerator()
    code = model_generator.render(schema=TestModel.generate_schema())
    assert (
        code
        == r"""from dataclasses_avroschema import AvroModel
import dataclasses
import typing


@dataclasses.dataclass
class TestModel(AvroModel):
    data: str
    optional_data: typing.Optional[str] = None
"""
    )
