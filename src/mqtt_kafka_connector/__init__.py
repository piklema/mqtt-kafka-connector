from logging import config
from mqtt_kafka_connector.conf import LOGGING

config.dictConfig(LOGGING)
