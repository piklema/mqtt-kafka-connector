import pytest
from dotenv import load_dotenv

from connector import Connector


@pytest.fixture
def conn():
    conn = Connector()
    return conn


load_dotenv('.env.example')
