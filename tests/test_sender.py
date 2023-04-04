import datetime as dt
import types
from io import StringIO
from unittest.mock import AsyncMock, patch

import pytest

from connector.send import (
    TruckTelemetry,
    TruckTelemetryList,
    parse_conf_file,
    read_telemetry_data,
    send_test_data,
)


@pytest.fixture
def config_content():
    return """
# Экскаваторы
102, 1
103, 2
104, 3
106, 4
# Самосвалы
894, 11
895, 12
896, 13
897, 14
898, 15
899, 16
900, 17
901, 18
902, 19
903, 20
""".splitlines()


@pytest.fixture
def data_content() -> StringIO:
    return StringIO(
        """
time,objectid,weight_dynamic,accelerator_position,height,lat,lon,speed,course
2023-02-07 10:00:00,894,186.0,0.0,820.0,51.51025,118.58797,0.3,0.0
2023-02-07 10:00:00,895,0.0,0.8,929.0,51.483467,118.53613,0.0,0.0
""".strip()
    )


def test_parse_config(config_content):
    content = parse_conf_file(config_content)
    assert len(content) == 14


def test_read_telemetry_data(config_content, data_content):
    conf_dict = parse_conf_file(config_content)
    gen = read_telemetry_data(data_content, conf_dict)
    assert isinstance(gen, types.GeneratorType)
    truck = next(gen)
    assert truck.object_id == 11
    assert next(gen).object_id == 12


@pytest.mark.asyncio
async def test_send_test_data(config_content, data_content):
    conf_dict = parse_conf_file(config_content)
    args = types.SimpleNamespace()
    args.customer_id = 1
    args.schema_id = 1
    args.infinite = False
    with patch('connector.send.aiomqtt.Client') as mock_client:
        mock_client.return_value = AsyncMock()
        mock_client.return_value.publish = AsyncMock()
        await send_test_data(data_content, conf_dict, args)
        assert mock_client.call_count == 1
        assert mock_client.return_value.publish.call_count == 2


def test_avro_serialization():
    ttl = TruckTelemetryList(
        data=[
            TruckTelemetry(
                time=dt.datetime.now(),
                object_id=894,
                weight_dynamic=186.0,
                accelerator_position=0.0,
                height=820.0,
                lat=51.51025,
                lon=118.58797,
                speed=0.3,
                course=0.0,
            )
        ]
    )
    payload = ttl.serialize()
    assert type(payload) == bytes
