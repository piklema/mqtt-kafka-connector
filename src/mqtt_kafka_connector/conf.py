import logging.config
import os
import uuid

import sentry_sdk
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration

load_dotenv()

MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_RECONNECT_INTERVAL_SEC = int(os.getenv('MQTT_RECONNECT_INTERVAL_SEC'))
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID') or uuid.uuid4().hex
MQTT_TOPIC_SOURCE_MATCH = os.getenv('MQTT_TOPIC_SOURCE_MATCH')
MQTT_TOPIC_SOURCE_TEMPLATE = os.getenv('MQTT_TOPIC_SOURCE_TEMPLATE')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_TEMPLATE = os.getenv('KAFKA_TOPIC_TEMPLATE')
KAFKA_KEY_TEMPLATE = os.getenv('KAFKA_KEY_TEMPLATE')
KAFKA_HEADERS_LIST = os.getenv('KAFKA_HEADERS_LIST')
TRACE_HEADER = os.getenv('TRACE_HEADER')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
SCHEMA_REGISTRY_REQUEST_HEADERS = os.getenv('SCHEMA_REGISTRY_REQUEST_HEADERS')
MESSAGE_DESERIALIZE = bool(
    SCHEMA_REGISTRY_URL and SCHEMA_REGISTRY_REQUEST_HEADERS
)

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
    )

fmt = (
    '%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).'
    '%(funcName)s:%(lineno)d - %(message)s'
)
logging.basicConfig(level=logging.INFO, format=fmt)
