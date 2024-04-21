from unittest.mock import patch

import pytest
from mqtt_kafka_connector.clients.base_http import BaseHTTPClient
from mqtt_kafka_connector.clients.schema_client import SchemaClient

from tests.conftest import DummyResponse

SCHEMA_URL = 'https://domain.com/api/v1/schemas'


@patch('httpx.AsyncClient.request')
async def test_http_client(httpx_mock):
    params = {'test': 'test'}
    httpx_mock.return_value = DummyResponse(200, params)
    client = BaseHTTPClient(headers={'Authorization': 'Token 123'})
    resp_json = await client.request(
        url=SCHEMA_URL, method='get', params=params
    )
    assert resp_json == params

    # test 404
    httpx_mock.return_value = DummyResponse(404, params)
    with pytest.raises(RuntimeError) as excinfo:
        await client.get(url=SCHEMA_URL, params=params)

    assert '404' in str(excinfo.value)


@patch(
    'mqtt_kafka_connector.clients.schema_client.SCHEMA_REGISTRY_URL',
    SCHEMA_URL,
)
@patch('httpx.AsyncClient.request')
async def test_schema_client(http_mock):
    data = {'test': 'test'}
    http_mock.return_value = DummyResponse(200, data)

    client = SchemaClient()

    resp = await client.get_schema(schema_id=1)
    assert resp == data
    assert http_mock.call_count == 1
    assert http_mock.call_args[0][1] == f'{SCHEMA_URL}/1'


@patch('httpx.AsyncClient.request')
async def test_lru(http_mock):
    data = {'test': 'test'}
    http_mock.return_value = DummyResponse(200, data)

    client = SchemaClient()
    res = await client.get_schema(1)
    assert res
    assert http_mock.call_count == 1

    res = await client.get_schema(2)
    assert res
    assert http_mock.call_count == 2

    res = await client.get_schema(1)
    assert res
    assert http_mock.call_count == 2
