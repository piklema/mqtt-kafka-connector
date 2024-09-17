import functools
import logging

from aioprometheus import Counter, Summary
from aioprometheus.service import Service
from mqtt_kafka_connector.conf import PROMETHEUS_PORT
from mqtt_kafka_connector.context_vars import customer_id_var, device_id_var

logger = logging.getLogger(__name__)


class Prometheus:
    def __init__(self):
        self.service = None
        self.messages_counter = Counter(
            'messages_from_devices_count',
            'Count of messages.',
        )
        self.telemetry_message_lag = Summary(
            'telemetry_message_lag_seconds',
            'Time lag between message time and current time.',
        )

    async def start(self):
        self.service = Service()
        await self.service.start(addr='0.0.0.0', port=int(PROMETHEUS_PORT))
        logger.info('Prometheus Service is running')

    def _add(self, metric, value: float):
        getattr(self, metric).add(
            {
                'device_id': str(device_id_var.get()),
                'customer_id': str(customer_id_var.get()),
            },
            value=value,
        )

    messages_counter_add = functools.partialmethod(
        _add, metric='messages_counter'
    )
    telemetry_message_lag_add = functools.partialmethod(
        _add, metric='telemetry_message_lag'
    )
