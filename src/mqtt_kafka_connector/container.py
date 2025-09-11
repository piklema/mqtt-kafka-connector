"""DI-контейнер.

В этом файле определяется DI-контейнер для управления зависимостями приложения.
"""
from unittest.mock import AsyncMock

from dependency_injector import containers, providers

from mqtt_kafka_connector.clients.kafka import KafkaProducer, MessageHelper
from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.clients.schema_client import SchemaClient
from mqtt_kafka_connector.connector.connector import Connector
from mqtt_kafka_connector.connector.handlers import (
    FStateHandler,
    TelemetryHandler,
    TopicRouter,
)
from mqtt_kafka_connector.middlewares import (
    AvroMiddleware,
    GzipMiddleware,
    JsonMiddleware,
    Pipeline,
)
from mqtt_kafka_connector.services.prometheus import Prometheus
from mqtt_kafka_connector.settings import settings


class Container(containers.DeclarativeContainer):
    """DI-контейнер приложения."""

    config = providers.Configuration()
    config.from_pydantic(settings)

    prometheus = providers.Singleton(
        Prometheus,
    )

    message_helper = providers.Singleton(
        MessageHelper,
        prometheus=prometheus,
    )

    kafka_producer = providers.Singleton(
        KafkaProducer,
        message_helper=message_helper,
    )

    mqtt_client = providers.Singleton(MQTTClient)

    schema_client = providers.Selector(
        config.E2E_TESTING,
        true=providers.Factory(
            lambda: AsyncMock(
                get_schema=AsyncMock(
                    return_value={
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
                )
            )
        ),
        false=providers.Singleton(SchemaClient),
    )

    # Middlewares
    gzip_middleware = providers.Singleton(GzipMiddleware)
    avro_middleware = providers.Singleton(
        AvroMiddleware,
        schema_client=schema_client,
    )
    json_middleware = providers.Singleton(JsonMiddleware)

    telemetry_pipeline = providers.Singleton(
        Pipeline,
        middlewares=providers.List(
            gzip_middleware,
            avro_middleware,
            json_middleware,
        ),
    )

    fstate_handler = providers.Singleton(
        FStateHandler,
        kafka_producer=kafka_producer,
    )

    telemetry_handler = providers.Singleton(
        TelemetryHandler,
        kafka_producer=kafka_producer,
        pipeline=telemetry_pipeline,
        prometheus=prometheus,
    )

    topic_router = providers.Singleton(
        TopicRouter,
        telemetry_handler=telemetry_handler,
        fstate_handler=fstate_handler,
    )

    connector = providers.Singleton(
        Connector,
        mqtt_client=mqtt_client,
        kafka_producer=kafka_producer,
        topic_router=topic_router,
        prometheus=prometheus,
    )
