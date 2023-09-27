import logging

from aiocache import cached

from mqtt_kafka_connector.clients.base_http import BaseHTTPClient
from mqtt_kafka_connector.conf import (
    SCHEMA_CACHE_TTL,
    SCHEMA_REGISTRY_REQUEST_HEADERS,
    SCHEMA_REGISTRY_URL,
)

logger = logging.getLogger(__name__)


class SchemaClient(BaseHTTPClient):
    @cached(ttl=SCHEMA_CACHE_TTL)
    async def get_schema(self, schema_id: int) -> dict:
        return await self.get(f'{SCHEMA_REGISTRY_URL}/{schema_id}')


headers = (
    {
        k: v
        for k, v in [
            h.split(":") for h in SCHEMA_REGISTRY_REQUEST_HEADERS.split(",")
        ]
    }
    if SCHEMA_REGISTRY_REQUEST_HEADERS
    else None
)

schema_client = SchemaClient(headers=headers)
