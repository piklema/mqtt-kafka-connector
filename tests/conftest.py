import pytest

from connector import Connector


@pytest.fixture
def conn():
    conn = Connector()
    return conn
