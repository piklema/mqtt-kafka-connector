from io import StringIO
import types
import pytest
from connector.send import parse_conf_file, send_test_data, read_telemetry_data


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
