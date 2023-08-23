import functools
import logging
import typing
from json import JSONDecodeError

import httpx

logger = logging.getLogger(__name__)


class BaseHTTPClient:
    def __init__(self, headers: dict):
        self.headers = headers

    async def request(self, url: str, method: str, **kwargs) -> dict:
        logger.error(f'HTTP request: {method=}, {url=}, {kwargs=}')
        async with httpx.AsyncClient(
            headers=self.headers, timeout=1
        ) as client:
            try:
                resp = await getattr(client, method)(
                    url, headers=self.headers, **kwargs
                )
                resp_json = resp.json()

                logger.error(f'HTTP response: {resp_json}')

                if resp.status_code not in [
                    httpx.codes.OK,
                    httpx.codes.CREATED,
                ]:
                    raise RuntimeError(
                        f'Failed request with status {resp.status_code} '
                        f'error {resp_json}'
                    )

                return resp_json

            except (httpx.HTTPError, JSONDecodeError) as e:
                logger.error(f'HTTP error: {e}')

    get: typing.Callable = functools.partialmethod(request, method='get')
