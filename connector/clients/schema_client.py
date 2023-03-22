from connector.clients.base_http import BaseHTTPClient
from connector.conf import SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_REQUEST_HEADERS
from functools import lru_cache


class SchemaClient(BaseHTTPClient):
    @lru_cache()
    async def get_schema(self, schema_id: int) -> dict:
        return await self.get(path=f"/api/v1/schemas/{schema_id}")


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


schema_client = SchemaClient(
    base_url=SCHEMA_REGISTRY_URL,
    headers=headers,
)
