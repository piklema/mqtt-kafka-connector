import logging

from aiocache import cached

from mqtt_kafka_connector.clients.base_http import BaseHTTPClient
from mqtt_kafka_connector.settings import settings

logger = logging.getLogger(__name__)

HEADERS = (
    dict([h.split(":") for h in settings.SCHEMA_REGISTRY_REQUEST_HEADERS.split(",")])
    if settings.SCHEMA_REGISTRY_REQUEST_HEADERS
    else None
)


class SchemaClient(BaseHTTPClient):
    def __init__(self):
        super().__init__(headers=HEADERS)

    @cached(ttl=settings.SCHEMA_CACHE_TTL)
    async def get_schema(self, schema_id: int) -> dict:
        return await self.get(f"{settings.SCHEMA_REGISTRY_URL}/{schema_id}")