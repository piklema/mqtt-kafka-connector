import asyncio
from io import BytesIO

import asyncio_mqtt as aiomqtt
import fastavro

from mqtt_kafka_connector import conf

SCHEMA = {
    "type": "record",
    "name": "MessagesPacket",
    "fields": [
        {
            "name": "messages",
            "type": {
                "type": "array",
                "items": {
                    "doc": "Device Message",
                    "name": "Message",
                    "type": "record",
                    "fields": [
                        {
                            "name": "time",
                            "type": {
                                "type": "long",
                                "logicalType": "timestamp-millis",
                            },
                        },
                        {
                            "name": "speed",
                            "type": ["null", "double"],
                            "default": None,
                        },
                        {
                            "name": "acceleration",
                            "type": ["null", "double"],
                            "default": None,
                        },
                    ],
                },
                "name": "message",
            },
        }
    ],
    "doc": "MessagesPacket",
}

DATA = dict(
    messages=[
        dict(
            time=1683072000011,
            speed=41.23,
            acceleration=0.1,
        )
    ]
)


def serialize(schema, data) -> bytes:
    fastavro.validate(data, schema)
    parsed_schema = fastavro.parse_schema(SCHEMA)
    buffer = BytesIO()
    fastavro.schemaless_writer(buffer, parsed_schema, data)
    buffer.seek(0)
    return buffer.read()


async def main():
    data = serialize(SCHEMA, DATA)
    async with aiomqtt.Client(
        hostname=conf.MQTT_HOST,
        port=conf.MQTT_PORT,
        username=conf.MQTT_USER,
        password=conf.MQTT_PASSWORD,
        client_id=conf.MQTT_CLIENT_ID,
        clean_session=False,
    ) as client:
        await client.publish('customer/1/dev/2/v3', payload=data, qos=2)


if __name__ == "__main__":
    asyncio.run(main())
