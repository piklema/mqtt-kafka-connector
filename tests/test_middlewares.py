import gzip
from unittest import mock

import fastavro
import orjson
import pytest

from mqtt_kafka_connector.middlewares import (
    AvroMiddleware,
    GzipMiddleware,
    JsonMiddleware,
    Pipeline,
)


@pytest.mark.asyncio
class TestGzipMiddleware:
    async def test_process_gzipped(self):
        """Тест проверяет, что мидлварь корректно разархивирует Gzip."""
        payload = b"test_payload"
        gzipped_payload = gzip.compress(payload)
        middleware = GzipMiddleware()

        result = await middleware.process(gzipped_payload)

        assert result == payload

    async def test_process_not_gzipped(self):
        """Тест проверяет, что мидлварь не изменяет не-Gzip нагрузку."""
        payload = b"test_payload"
        middleware = GzipMiddleware()

        result = await middleware.process(payload)

        assert result == payload


@pytest.mark.asyncio
class TestAvroMiddleware:
    async def test_process_avro_success(self, schema_client):
        """Тест успешной десериализации Avro."""
        # Этот тест упрощен. Реальная Avro-нагрузка бинарна.
        payload = b"avro_payload"
        schema_id = 1
        schema = {"type": "record", "name": "test", "fields": []}
        schema_client.get_schema.return_value = schema
        middleware = AvroMiddleware(schema_client)

        with mock.patch("fastavro.schemaless_reader", return_value={"a": 1}):
            result = await middleware.process(payload, schema_id=schema_id)

        assert result == {"a": 1}
        schema_client.get_schema.assert_called_once_with(schema_id)

    async def test_process_no_schema_id(self, schema_client):
        """Тест проверяет, что мидлварь пропускает нагрузку, если нет schema_id."""
        payload = b"avro_payload"
        middleware = AvroMiddleware(schema_client)

        result = await middleware.process(payload)

        assert result == payload
        schema_client.get_schema.assert_not_called()

    async def test_process_avro_fail(self, schema_client):
        """Тест проверяет, что при ошибке десериализации возвращается исходная нагрузка."""
        payload = b"invalid_avro"
        schema_id = 1
        schema = {"type": "record", "name": "test", "fields": []}
        schema_client.get_schema.return_value = schema
        middleware = AvroMiddleware(schema_client)

        with mock.patch(
            "fastavro.schemaless_reader",
            side_effect=fastavro.validation.ValidationError("bad avro"),
        ):
            result = await middleware.process(payload, schema_id=schema_id)

        assert result == payload


@pytest.mark.asyncio
class TestJsonMiddleware:
    async def test_process_json_success(self):
        """Тест успешной десериализации JSON."""
        payload = orjson.dumps({"a": 1})
        middleware = JsonMiddleware()

        result = await middleware.process(payload)

        assert result == {"a": 1}

    async def test_process_json_fail(self):
        """Тест проверяет, что при ошибке десериализации JSON возвращается исходная нагрузка."""
        payload = b"not a json"
        middleware = JsonMiddleware()

        result = await middleware.process(payload)

        assert result == payload

    async def test_process_already_dict(self):
        """Тест проверяет, что мидлварь не обрабатывает уже десериализованные данные."""
        payload = {"a": 1}
        middleware = JsonMiddleware()

        with mock.patch("orjson.loads") as mock_orjson_loads:
            result = await middleware.process(payload)

            assert result == payload
            mock_orjson_loads.assert_not_called()


@pytest.mark.asyncio
class TestPipeline:
    async def test_pipeline_runs_in_order(self):
        """Тест проверяет, что мидлвари в конвейере вызываются по порядку."""
        m1 = mock.AsyncMock(spec=GzipMiddleware)
        m2 = mock.AsyncMock(spec=AvroMiddleware)
        m1.process.return_value = b"m1_processed"
        m2.process.return_value = b"m2_processed"

        pipeline = Pipeline(middlewares=[m1, m2])
        result = await pipeline.run(b"start")

        m1.process.assert_called_once_with(b"start", **{})
        m2.process.assert_called_once_with(b"m1_processed", **{})
        assert result == b"m2_processed"

    async def test_pipeline_stops_on_dict(self):
        """Тест проверяет, что конвейер останавливается, когда мидлварь возвращает dict."""
        m1 = mock.AsyncMock(spec=GzipMiddleware)
        m2 = mock.AsyncMock(spec=AvroMiddleware)
        m1.process.return_value = {"a": 1}  # m1 возвращает dict

        pipeline = Pipeline(middlewares=[m1, m2])
        result = await pipeline.run(b"start")

        m1.process.assert_called_once_with(b"start", **{})
        m2.process.assert_not_called()  # m2 не должна быть вызвана
        assert result == {"a": 1}
