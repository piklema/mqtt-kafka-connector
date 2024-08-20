import asyncio
import dataclasses
import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiomqtt.message import Message
from aioprometheus import REGISTRY
from dataclasses_avroschema import AvroModel
from mqtt_kafka_connector.clients.kafka import KafkaProducer, MessageHelper
from mqtt_kafka_connector.services.prometheus import Prometheus


@pytest.fixture()
def now_timestamp():
    return int(
        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1_000
    )


@pytest.fixture()
def payload(now_timestamp):
    return dict(
        messages=[
            dict(
                time=now_timestamp,
                speed=10.00,
                lat=11.2222,
                lon=22.3333,
            ),
            dict(
                time=now_timestamp,
                speed=33.00,
                lat=55.5555,
                lon=77.9999,
            ),
        ],
    )


@dataclasses.dataclass
class MessageModel(AvroModel):
    time: datetime.datetime
    speed: float
    lat: float
    lon: float


@dataclasses.dataclass
class MessagePack(AvroModel):
    messages: list[MessageModel]


class DummyResponse:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data

    def raise_for_status(self):
        pass


@pytest.fixture()
def message_pack(payload):
    return MessagePack(**payload)


@pytest.fixture()
def unpack_message_pack(payload, now_timestamp):
    return [{'time': datetime.datetime.now()}] * 2


@pytest.fixture()
def schema():
    return MessagePack.generate_schema()


@pytest.fixture
async def prometheus():
    service = Prometheus()
    service.start = AsyncMock()
    service._add = MagicMock()
    yield service
    REGISTRY.clear()


@pytest.fixture()
def kafka_producer(prometheus):
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer.create_batch = MagicMock()
    mock_producer.create_batch.return_value.append.side_effect = [
        None,
        '1',
        'metadata',
    ]
    mock_producer.partitions_for = AsyncMock(return_value=[0])

    res = MagicMock()
    res.partition = 1

    mock_producer.send_batch.return_value = asyncio.Future()
    mock_producer.send_batch.return_value.set_result(res)

    mock_producer.send_and_wait = AsyncMock()
    mock_producer.send_and_wait.return_value = res
    message_helper = MessageHelper(prometheus)
    kafka_producer = KafkaProducer(message_helper)
    kafka_producer.producer = mock_producer
    return kafka_producer


@pytest.fixture
async def mqtt_client(monkeypatch, message_pack):
    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.messages.__aiter__.return_value = iter(
        [
            Message(
                'topic',
                payload=message_pack.serialize(),
                qos=1,
                retain=True,
                mid=1,
                properties=None,
            )
        ]
    )
    mock_client.subscribe = AsyncMock()

    mqtt_client = MagicMock(return_value=mock_client)
    monkeypatch.setattr('aiomqtt.Client', mqtt_client)
    return mqtt_client
