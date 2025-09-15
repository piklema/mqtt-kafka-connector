from mqtt_kafka_connector.container import Container
from mqtt_kafka_connector.connector.handlers import FStateHandler, TelemetryHandler


def test_container_wiring():
    """
    Тест проверяет, что DI-контейнер создается и правильно связывает зависимости.
    """
    container = Container()

    fstate_handler = container.fstate_handler()
    telemetry_handler = container.telemetry_handler()

    assert isinstance(fstate_handler, FStateHandler)
    assert isinstance(telemetry_handler, TelemetryHandler)
    assert fstate_handler.pipeline is telemetry_handler.pipeline
    assert fstate_handler.kafka_producer is telemetry_handler.kafka_producer