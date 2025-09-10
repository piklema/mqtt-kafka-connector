"""DI-контейнер.

В этом файле определяется DI-контейнер для управления зависимостями приложения.
"""

from dependency_injector import containers, providers

from mqtt_kafka_connector import conf
from mqtt_kafka_connector.clients.kafka import KafkaProducer, MessageHelper
from mqtt_kafka_connector.clients.mqtt import MQTTClient
from mqtt_kafka_connector.clients.schema_client import SchemaClient
from mqtt_kafka_connector.connector.connector import Connector
from mqtt_kafka_connector.connector.handlers import (
    FStateHandler,
    TelemetryHandler,
    TopicRouter,
)
from mqtt_kafka_connector.services.prometheus import Prometheus


class Container(containers.DeclarativeContainer):
    """DI-контейнер приложения."""

    config = providers.Configuration()
    config.from_dict(conf.__dict__)

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

    schema_client = providers.Singleton(SchemaClient)

    fstate_handler = providers.Singleton(
        FStateHandler,
        kafka_producer=kafka_producer,
    )

    telemetry_handler = providers.Singleton(
        TelemetryHandler,
        kafka_producer=kafka_producer,
        schema_client=schema_client,
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