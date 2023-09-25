from unittest import mock

import pytest

from mqtt_kafka_connector.connector import Connector


@pytest.fixture
def conn():
    conn = Connector()
    conn.producer = mock.AsyncMock()
    return conn


class DummyResponse:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data
