from unittest.mock import patch

import pytest

from connector.clients.base_http import BaseHTTPClient
from connector.clients.schema_client import SchemaClient

from conftest import DummyResponse


@patch('httpx.AsyncClient.request')
async def test_http_client(httpx_mock):
    data = {'test': 'test'}
    httpx_mock.return_value = DummyResponse(200, data)
    client = BaseHTTPClient(
        base_url='piklema.com', headers={'Authorization': 'Token 123'}
    )
    resp_json = await client.request(
        path='/api/v1/schemas/1', method='get', params=data
    )
    assert resp_json == data

    # test 404
    httpx_mock.return_value = DummyResponse(404, data)
    with pytest.raises(RuntimeError) as excinfo:
        await client.get(path='/api/v1/schemas/1', params=data)

    assert '404' in str(excinfo.value)


@patch('httpx.AsyncClient.request')
async def test_piklema_client(http_mock):
    data = {'test': 'test'}
    http_mock.return_value = DummyResponse(200, data)

    client = SchemaClient(
        base_url='https://piklema.com',
        headers={'Authorization': 'Token 123'},
    )

    resp = await client.get_schema(schema_id=1)
    assert resp == data
    assert http_mock.call_count == 1
    assert http_mock.call_args[0][1] == 'https://piklema.com/api/v1/schemas/1'
    assert http_mock.call_args[1]['headers'] == {'Authorization': 'Token 123'}
