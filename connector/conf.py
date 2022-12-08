import logging.config
import os
import uuid

from dotenv import load_dotenv

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
KAFKA_HEADERS_LIST = os.getenv('KAFKA_HEADERS_LIST')

LOGGING = {
    'version': 1,
    'root': {
        'level': 'DEBUG',
        'handlers': ['console'],
    },
    'formatters': {
        'verbose': {
            'format': '%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s:%(lineno)d - %(message)s'  # noqa
        },
    },
    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'logging.NullHandler',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

logging.config.dictConfig(LOGGING)
