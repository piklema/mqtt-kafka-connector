import asyncio
import datetime
import gzip
import io

import fastavro
import orjson
import pytest
from aiokafka import AIOKafkaConsumer
from aiomqtt import Client as MqttClient

from mqtt_kafka_connector.settings import settings
from tests.conftest import CUSTOMER_ID, DEVICE_ID, SCHEMA_ID


@pytest.fixture
def schema():
    return {
        "name": "MessagePack",
        "type": "record",
        "fields": [
            {
                "name": "messages",
                "type": {
                    "type": "array",
                    "items": {
                        "name": "MessageModel",
                        "type": "record",
                        "fields": [
                            {
                                "name": "time",
                                "type": {
                                    "type": "long",
                                    "logicalType": "timestamp-millis",
                                },
                            },
                            {"name": "speed", "type": "double"},
                            {"name": "lat", "type": "double"},
                            {"name": "lon", "type": "double"},
                        ],
                    },
                },
            }
        ],
    }


@pytest.fixture
def mock_schema_registry(mocker, schema):
    """Мок для запроса схемы."""
    mock = mocker.patch(
        "mqtt_kafka_connector.clients.schema_client.SchemaClient.get_schema",
        new_callable=mocker.AsyncMock,
    )
    mock.return_value = schema
    return mock


@pytest.fixture
async def mqtt_client():
    async with MqttClient(
        hostname=settings.MQTT_HOST,
        port=settings.MQTT_PORT,
        username=settings.MQTT_USER,
        password=settings.MQTT_PASSWORD,
    ) as client:
        yield client


@pytest.fixture
async def kafka_consumer_factory():
    consumers = []

    async def _factory(group_id: str) -> AIOKafkaConsumer:
        consumer = AIOKafkaConsumer(
            settings.TELEMETRY_KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        consumers.append(consumer)
        return consumer

    yield _factory

    for consumer in consumers:
        await consumer.stop()


async def consume_and_check(consumer: AIOKafkaConsumer, expected_speed: float):
    while True:
        msg = await asyncio.wait_for(consumer.getone(), timeout=5)
        assert msg is not None
        data = orjson.loads(msg.value)
        if data.get("speed") == expected_speed:
            return data


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_avro(
    schema,
    mqtt_client,
    kafka_consumer_factory,
    mock_schema_registry,
):
    """Тест сквозной отправки бинарного сообщения Avro."""
    consumer = await kafka_consumer_factory("test-group-avro")
    expected_speed = 10.0

    parsed_schema = fastavro.parse_schema(schema)
    now_millis = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    payload_dict = {
        "messages": [
            {
                "time": now_millis,
                "speed": expected_speed,
                "lat": 55.75,
                "lon": 37.61,
            },
        ]
    }
    fp = io.BytesIO()
    fastavro.schemaless_writer(fp, parsed_schema, payload_dict)
    fp.seek(0)
    payload = fp.read()

    await mqtt_client.publish(
        f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}",
        payload=payload,
    )

    data = await consume_and_check(consumer, expected_speed)
    assert data["speed"] == expected_speed
    assert data["lat"] == 55.75


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_json(mqtt_client, kafka_consumer_factory, mock_schema_registry):
    """Тест сквозной отправки сообщения в формате JSON."""
    consumer = await kafka_consumer_factory("test-group-json")
    expected_speed = 20.0

    now_millis = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    payload_dict = {
        "messages": [
            {
                "time": now_millis,
                "speed": expected_speed,
                "lat": 55.75,
                "lon": 37.61,
            },
        ]
    }
    payload = orjson.dumps(payload_dict)

    await mqtt_client.publish(
        f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}",
        payload=payload,
    )

    data = await consume_and_check(consumer, expected_speed)
    assert data["speed"] == expected_speed


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_gzipped_avro(
    schema,
    mqtt_client,
    kafka_consumer_factory,
    mock_schema_registry,
):
    """Тест сквозной отправки Gzipped Avro сообщения."""
    consumer = await kafka_consumer_factory("test-group-gzip")
    expected_speed = 30.0

    parsed_schema = fastavro.parse_schema(schema)
    now_millis = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    payload_dict = {
        "messages": [
            {
                "time": now_millis,
                "speed": expected_speed,
                "lat": 55.75,
                "lon": 37.61,
            },
        ]
    }
    fp = io.BytesIO()
    fastavro.schemaless_writer(fp, parsed_schema, payload_dict)
    fp.seek(0)
    avro_payload = fp.read()
    gzipped_payload = gzip.compress(avro_payload)

    await mqtt_client.publish(
        f"customer/{CUSTOMER_ID}/dev/{DEVICE_ID}/v{SCHEMA_ID}",
        payload=gzipped_payload,
    )

    data = await consume_and_check(consumer, expected_speed)
    assert data["speed"] == expected_speed
