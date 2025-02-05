from unittest.mock import MagicMock

import pytest
from mqtt_kafka_connector.clients.schema_registry import RetentionCache


@pytest.fixture
def retention_cache():
    return RetentionCache(default_retention=10)


def test_setitem(retention_cache):
    retention_cache.now = MagicMock(return_value=100)
    retention_cache.cleanup = MagicMock()
    retention_cache[1] = {'data': 'value'}
    assert retention_cache.cache[1] == {'data': 'value'}
    assert retention_cache.retention[1] == 100
    retention_cache.cleanup.assert_called_once()


def test_getitem(retention_cache):
    retention_cache.now = MagicMock(return_value=100)
    retention_cache.cleanup = MagicMock()
    retention_cache.cache[1] = {'data': 'value'}
    retention_cache.retention[1] = 90
    assert retention_cache[1] == {'data': 'value'}
    retention_cache.cleanup.assert_called_once()


def test_getitem_expired(retention_cache):
    retention_cache.now = MagicMock(return_value=100)
    retention_cache.cache[1] = {'data': 'value'}
    retention_cache.retention[1] = 90
    retention_cache.default_retention = 10
    with pytest.raises(KeyError):
        retention_cache[1]
    assert 1 not in retention_cache.cache
    assert 1 not in retention_cache.retention


def test_cleanup(retention_cache):
    retention_cache.now = MagicMock(return_value=100)
    retention_cache.cache[1] = {'data': 'value'}
    retention_cache.retention[1] = 90
    retention_cache.default_retention = 10
    retention_cache.cleanup()
    assert 1 not in retention_cache.cache
    assert 1 not in retention_cache.retention
