import logging.config
import os
import uuid

MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_USER = os.getenv('MQTT_USER', 'collector')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', 'collector')
MQTT_RECONNECT_INTERVAL_SEC = os.getenv('MQTT_RECONNECT_INTERVAL_SEC', 2)
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', uuid.uuid4().hex)
MQTT_TOPIC_SOURCE_MATCH = os.getenv('MQTT_CLIENT_ID', 'customer/+/dev/+')

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'
)


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
