import json
import types
from io import StringIO
from unittest.mock import patch

import pytest

from mqtt_kafka_connector.emu import (
    parse_conf_file,
    read_telemetry_data,
    send_test_data,
)

DEVICE_ID_1 = 11
DEVICE_ID_2 = 12


@pytest.fixture
def config_content():
    return f"""
# Экскаваторы
102, 1
103, 2
104, 3
106, 4
# Самосвалы
894, {DEVICE_ID_1}
895, {DEVICE_ID_2}
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
    assert truck.device_id == 11
    assert next(gen).device_id == 12


@pytest.mark.asyncio
async def test_send_test_data(config_content, data_content):
    conf_dict = parse_conf_file(config_content)
    args = types.SimpleNamespace()
    args.customer_id = 1
    args.schema_id = 1
    args.infinite = False
    with patch(
        'mqtt_kafka_connector.connector.Connector.send_to_kafka'
    ) as mock_send:
        mock_send.return_value = True
        await send_test_data(data_content, conf_dict, args)
        assert mock_send.call_count == 2  # data_content has 2 lines

        assert mock_send.mock_calls[0].args[0] == 'telemetry'
        assert type(mock_send.mock_calls[0].args[1]) == bytes
        data = json.loads(mock_send.mock_calls[0].args[1])
        assert data['device_id'] == DEVICE_ID_1
        assert 'time' in data
        assert 'weight_dynamic' in data
        assert 'accelerator_position' in data
        assert 'height' in data
        assert 'lat' in data
        assert 'lon' in data
        assert 'speed' in data
        assert 'course' in data

        assert mock_send.mock_calls[1].args[0] == 'telemetry'
        assert type(mock_send.mock_calls[1].args[1]) == bytes
        data = json.loads(mock_send.mock_calls[1].args[1])
        assert data['device_id'] == DEVICE_ID_2