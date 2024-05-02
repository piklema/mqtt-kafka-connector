import logging

from aioprometheus import Counter
from aioprometheus.service import Service
from mqtt_kafka_connector.conf import PROMETHEUS_PORT

logger = logging.getLogger(__name__)


class Prometheus:
    def __init__(self):
        self.service = None
        self.messages_counter = Counter(
            'messages_count', 'Number of messages.'
        )

    async def start(self):
        self.service = Service()
        await self.service.start(addr='0.0.0.0', port=PROMETHEUS_PORT)
        logger.info('Prometheus Service is running')

    def add(self, device_id: int, customer_id: int, messages_count: int):
        self.messages_counter.add(
            {
                'device_id': str(device_id),
                'customer_id': str(customer_id),
            },
            messages_count,
        )
