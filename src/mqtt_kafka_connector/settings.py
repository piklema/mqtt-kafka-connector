import logging
from logging import Filter

import sentry_sdk
from pydantic_settings import BaseSettings, SettingsConfigDict
from sentry_sdk.integrations.logging import LoggingIntegration

from mqtt_kafka_connector.context_vars import device_id_var, message_uuid_var


class MessageParamsFilter(Filter):
    def filter(self, record):
        message_uuid = message_uuid_var.get()
        record.device_id = device_id_var.get()
        record.message_uuid = message_uuid
        record.service_name = settings.SERVICE_NAME
        record.environment = settings.ENVIRONMENT
        return True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    LOGLEVEL: str = "INFO"
    MQTT_HOST: str
    MQTT_PORT: int = 1883
    MQTT_USER: str
    MQTT_PASSWORD: str
    RECONNECT_INTERVAL_SEC: int = 3
    MQTT_CLIENT_ID: str = "mqtt-kafka-connector-1"
    MQTT_TOPIC_SOURCE_MATCH: str
    MQTT_TOPIC_SOURCE_TEMPLATE: str
    MQTT_FSTATE_SOURCE_MATCH: str
    MQTT_FSTATE_SOURCE_TEMPLATE: str

    KAFKA_BOOTSTRAP_SERVERS: str
    TELEMETRY_KAFKA_TOPIC: str = "telemetry"
    FSTATE_KAFKA_TOPIC: str = "fstate"
    KAFKA_KEY_TEMPLATE: str
    KAFKA_HEADERS_LIST: str
    TRACE_HEADER: str
    SCHEMA_REGISTRY_URL: str
    SCHEMA_REGISTRY_REQUEST_HEADERS: str

    WITH_MESSAGE_DESERIALIZE: bool = True
    SCHEMA_CACHE_TTL: int = 60

    SERVICE_NAME: str = "piklema-mqtt-kafka-connector"
    ENVIRONMENT: str = ""

    SENTRY_DSN: str | None = None
    MODIFY_MESSAGE_RM_NONE_FIELDS: bool = True
    MODIFY_MESSAGE_RM_NON_NUMBER_FLOAT_FIELDS: bool = False
    KAFKA_SEND_BATCHES: bool = False
    PROMETHEUS_PORT: int = 8011
    MIN_TELEMETRY_INTERVAL_AGE_HOURS: int = 24 * 3
    MAX_TELEMETRY_INTERVAL_AGE_HOURS: int = 1
    RELEASE_VERSION: str = ""


settings = Settings()

if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        integrations=[
            LoggingIntegration(event_level=int(logging.WARNING)),
        ],
        traces_sample_rate=0.5,
        send_default_pii=True,
        attach_stacktrace=False,
        max_breadcrumbs=20,
        release=f"mqtt-kafka-connector@{settings.RELEASE_VERSION}",
        environment=settings.ENVIRONMENT,
    )


LOGGING = {
    "version": 1,
    "root": {
        "level": settings.LOGLEVEL,
        "handlers": ["console"],
    },
    "formatters": {
        "verbose": {
            "format": "%(asctime)s - [%(levelname)s] - %(name)s - "
            "%(pathname)s:%(lineno)d (%(funcName)s) - %(message)s",
        },
    },
    "handlers": {
        "null": {
            "level": settings.LOGLEVEL,
            "class": "logging.NullHandler",
        },
        "console": {
            "level": settings.LOGLEVEL,
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": settings.LOGLEVEL,
            "propagate": True,
        },
    },
    "filters": {
        "message_params": {
            "()": MessageParamsFilter,
        },
    },
}