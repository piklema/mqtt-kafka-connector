from unittest.mock import patch

from mqtt_kafka_connector.connector.main import main


@patch("mqtt_kafka_connector.connector.main.Container")
@patch("asyncio.run")
def test_main(mock_asyncio_run, mock_container):
    """
    Тест проверяет, что:
    - Создается DI-контейнер.
    - Из контейнера получается коннектор.
    - Запускается `asyncio.run` с методом `run` коннектора.
    """
    # Действие
    main()

    # Проверки
    mock_container.assert_called_once()
    mock_container.return_value.connector.assert_called_once()
    mock_asyncio_run.assert_called_once_with(
        mock_container.return_value.connector.return_value.run.return_value
    )
