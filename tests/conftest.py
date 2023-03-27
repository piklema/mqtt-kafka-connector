import pytest

from connector import Connector


@pytest.fixture
def conn():
    conn = Connector()
    return conn


class DummyResponse:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data
