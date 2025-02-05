from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from mqtt_kafka_connector import conf
from mqtt_kafka_connector.clients.schema_registry import SchemaRegistry


@pytest.fixture
def schema_registry():
    return SchemaRegistry()


def test_get_url(schema_registry):
    conf.SCHEMA_REGISTRY_HOST = 'http://example.com'
    assert schema_registry.get_url(123) == 'http://example.com/subjects/123'


async def test_get_schema_from_cache(schema_registry):
    schema_registry.cache[123] = {'schema': 'test_schema'}
    assert await schema_registry.get_schema(123) == {'schema': 'test_schema'}


@patch('mqtt_kafka_connector.clients.base_http.BaseHTTPClient.get')
def test_get_schema_from_network(mock_get, schema_registry):
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json.return_value = {'schema': 'test_schema'}
    mock_get.return_value = AsyncMock(return_value=mock_response)

    assert schema_registry.get_schema(123) == {'schema': 'test_schema'}
    assert schema_registry.cache[123] == {'schema': 'test_schema'}


# @patch('mqtt_kafka_connector.clients.base_http.BaseHTTPClient.get')
# async def test_get_schema_not_found(mock_get, schema_registry):
#     mock_response = AsyncMock()
#     mock_response.status = 404
#     mock_get.return_value = mock_response

#     with pytest.raises(Exception):
#         await schema_registry.get_schema(123)
