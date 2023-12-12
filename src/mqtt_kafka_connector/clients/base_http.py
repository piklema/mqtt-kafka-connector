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
        logger.info('HTTP request: %s %s %s', method, url, kwargs)
        async with httpx.AsyncClient(headers=self.headers, timeout=1) as client:
            try:
                resp = await getattr(client, method)(url, headers=self.headers, **kwargs)
                resp.raise_for_status()
                resp_json = resp.json()

                logger.info('HTTP response: %s', resp_json)

                if resp.status_code not in [
                    httpx.codes.OK,
                    httpx.codes.CREATED,
                ]:
                    raise RuntimeError(
                        'Failed request with status %s ' 'error %s',
                        resp.status_code,
                        resp_json,
                    )

                return resp_json

            except httpx.HTTPError as e:
                logger.error("HTTPError: %s", e)

            except JSONDecodeError as e:
                logger.error('JSONDecodeError: %s', e)

    get: typing.Callable = functools.partialmethod(request, method='get')
