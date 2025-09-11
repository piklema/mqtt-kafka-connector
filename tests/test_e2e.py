import asyncio
import datetime
import io

import fastavro
import orjson
import pytest
from aiokafka import AIOKafkaConsumer
from aiomqtt import Client as MqttClient

from mqtt_kafka_connector.settings import settings

DEVICE_ID = "22222"
SCHEMA_ID = "333333"
CUSTOMER_ID = "11111"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e():
    # 1. Запускаем брокеры Kafka и MQTT
    # Запустить сервисы нужно вручную командой:
    # `docker compose -f docker-compose.e2e.yml up -d`

    # 2. Отправляем сообщение в MQTT
    async with MqttClient(
        hostname=settings.MQTT_HOST,
        port=settings.MQTT_PORT,
        username=settings.MQTT_USER,
        password=settings.MQTT_PASSWORD,
    ) as mqtt_client:
        schema = {
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
        parsed_schema = fastavro.parse_schema(schema)

        now_millis = int(
            datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
        )
        payload_dict = {
            "messages": [
                {
                    "time": now_millis,
                    "speed": 10,
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

    # 3. Потребляем сообщение из Kafka
    consumer = AIOKafkaConsumer(
        settings.TELEMETRY_KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="test-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=5)
        assert msg is not None
        data = orjson.loads(msg.value)
        assert data["speed"] == 10
        assert data["lat"] == 55.75
    finally:
        await consumer.stop()
