import asyncio_mqtt as aiomqtt
from connector import conf
from dataclasses_avroschema import AvroModel

# Description: MQTT client for the connector
# serialize dataclass with avro and send to mqtt broker


class MQTTClient:
    def __init__(self, topic: str):
        self.topic = topic
        self.client = aiomqtt.Client(
            hostname=conf.MQTT_HOST,
            port=conf.MQTT_PORT,
            username=conf.MQTT_USER,
            password=conf.MQTT_PASSWORD,
            client_id=conf.MQTT_CLIENT_ID,
            clean_session=False,
        )

    def get_topic(self, schema_id: int) -> str:
        return conf.MQTT_TOPIC_TPML.format(
            customer_id=self.device_config['customer_id'],
            device_id=self.device_config['id'],
            schema_id=schema_id,
        )

    async def publish(self, data: AvroModel):
        payload = data.serialize()
        async with self.client as client:
            await client.publish(self.topic, payload)
