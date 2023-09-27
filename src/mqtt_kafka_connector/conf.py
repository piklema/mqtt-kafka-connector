import logging.config
import os
from logging import Filter

import sentry_sdk
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration

from mqtt_kafka_connector.context_vars import device_id_var, message_uuid_var

load_dotenv()

LOGLEVEL = os.getenv('LOGLEVEL', 'INFO')
MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
RECONNECT_INTERVAL_SEC = int(os.getenv('RECONNECT_INTERVAL_SEC', 3))
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID') or 'mqtt-kafka-connector-1'
MQTT_TOPIC_SOURCE_MATCH = os.getenv('MQTT_TOPIC_SOURCE_MATCH')
MQTT_TOPIC_SOURCE_TEMPLATE = os.getenv('MQTT_TOPIC_SOURCE_TEMPLATE')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TELEMETRY_KAFKA_TOPIC = os.getenv('TELEMETRY_KAFKA_TOPIC', 'telemetry')
KAFKA_KEY_TEMPLATE = os.getenv('KAFKA_KEY_TEMPLATE')
KAFKA_HEADERS_LIST = os.getenv('KAFKA_HEADERS_LIST')
TRACE_HEADER = os.getenv('TRACE_HEADER')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
SCHEMA_REGISTRY_REQUEST_HEADERS = os.getenv('SCHEMA_REGISTRY_REQUEST_HEADERS')
MESSAGE_DESERIALIZE = SCHEMA_REGISTRY_URL and SCHEMA_REGISTRY_REQUEST_HEADERS
SCHEMA_CACHE_TTL = int(os.getenv('SCHEMA_CACHE_TTL', 60))

SERVICE_NAME = 'piklema-mqtt-kafka-connector'
ENVIRONMENT = os.getenv('ENVIRONMENT', '')

SENTRY_DSN = os.getenv('SENTRY_DSN')

if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[
            LoggingIntegration(event_level=int(logging.WARNING)),
        ],
        traces_sample_rate=0.5,
        send_default_pii=True,
        attach_stacktrace=False,
        max_breadcrumbs=20,
        release='mqtt-kafka-connector@' + os.getenv('RELEASE_VERSION', ''),
        environment=ENVIRONMENT,
    )


class MessageParamsFilter(Filter):
    def filter(self, record):
        message_uuid = message_uuid_var.get()
        record.device_id = device_id_var.get()
        record.message_uuid = message_uuid
        record.service_name = SERVICE_NAME
        record.environment = ENVIRONMENT
        return True


LOGGING = {
    'version': 1,
    'root': {
        'level': LOGLEVEL,
        'handlers': ['console'],
    },
    'formatters': {
        'verbose': {
            'format': '%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s:%(lineno)d - %(message)s'  # noqa
        },
    },
    'handlers': {
        'null': {
            'level': LOGLEVEL,
            'class': 'logging.NullHandler',
        },
        'console': {
            'level': LOGLEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOGLEVEL,
            'propagate': False,
        },
    },
    'filters': {
        'message_params': {
            '()': MessageParamsFilter,
        },
    },
}
