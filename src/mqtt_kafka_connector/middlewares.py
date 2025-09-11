from __future__ import annotations

import abc
import gzip
import io
import logging
from typing import Any

import fastavro
import orjson

from mqtt_kafka_connector.clients.schema_client import SchemaClient

logger = logging.getLogger(__name__)

GZIP_SIGNATURE = b"\x1f\x8b"


class Middleware(abc.ABC):
    """Абстрактный базовый класс для мидлварей."""

    @abc.abstractmethod
    async def process(self, payload: bytes, **kwargs) -> bytes | dict:
        """
        Обрабатывает нагрузку сообщения.

        Args:
            payload: Нагрузка сообщения в виде байтов.
            **kwargs: Дополнительные параметры, которые могут понадобиться
                      мидлварям (например, schema_id для Avro).

        Returns:
            Обработанная нагрузка (может быть bytes или dict).
        """
        raise NotImplementedError


class Pipeline:
    """Конвейер для последовательного выполнения мидлварей."""

    def __init__(self, middlewares: list[Middleware]):
        self._middlewares = middlewares

    async def run(self, payload: bytes, **kwargs) -> Any:
        """
        Запускает конвейер обработки.

        Args:
            payload: Исходная нагрузка сообщения.
            **kwargs: Дополнительные параметры для мидлварей.

        Returns:
            Финальный результат обработки.
        """
        processed_payload = payload
        for middleware in self._middlewares:
            if not isinstance(processed_payload, bytes):
                # Предыдущая мидлварь уже распарсила данные
                break
            processed_payload = await middleware.process(processed_payload, **kwargs)

        return processed_payload


class GzipMiddleware(Middleware):
    """Мидлварь для разархивации Gzip-нагрузки."""

    async def process(self, payload: bytes, **kwargs) -> bytes | dict:
        if payload.startswith(GZIP_SIGNATURE):
            logger.debug("Gzip-сигнатура найдена. Разархивируем...")
            return gzip.decompress(payload)
        return payload


class AvroMiddleware(Middleware):
    """Мидлварь для десериализации Avro-нагрузки (без встроенной схемы)."""

    def __init__(self, schema_client: SchemaClient):
        self._schema_client = schema_client

    async def process(self, payload: bytes, **kwargs) -> bytes | dict:
        schema_id = kwargs.get("schema_id")
        if not schema_id:
            return payload

        try:
            schema = await self._schema_client.get_schema(schema_id)
            if not schema:
                logger.warning(
                    "Схема %s не найдена, невозможно декодировать Avro.",
                    schema_id,
                )
                return payload

            fp = io.BytesIO(payload)
            parsed_schema = fastavro.parse_schema(schema)
            data = fastavro.schemaless_reader(fp, parsed_schema)
            logger.debug("Сообщение десериализовано как Avro.")
            return data
        except (
            IndexError,
            StopIteration,
            EOFError,
            fastavro.validation.ValidationError,
        ) as e:
            logger.debug(
                "Не удалось декодировать как Avro со схемой %s: %s", schema_id, e
            )
            return payload


class JsonMiddleware(Middleware):
    """Мидлварь для обработки JSON-нагрузки."""

    async def process(self, payload: bytes | dict, **kwargs) -> bytes | dict:
        if isinstance(payload, dict):
            logger.debug("Сообщение уже десериализовано. Пропускаем обработку JSON.")
            return payload
        try:
            data = orjson.loads(payload)
            logger.debug("Сообщение десериализовано как JSON.")
            return data
        except orjson.JSONDecodeError:
            logger.debug("Не удалось декодировать как JSON. Пропускаем.")
            return payload
