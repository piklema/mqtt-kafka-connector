from unittest import mock

from mqtt_kafka_connector.connector.main import main


@mock.patch("mqtt_kafka_connector.connector.main.Container")
@mock.patch("asyncio.run")
def test_main_uses_container(mock_asyncio_run, mock_container):
    """
    Тест проверяет, что функция main использует DI-контейнер для создания
    и запуска коннектора.
    """
    # Arrange (Подготовка)
    # Моки asyncio.run и Container уже переданы в аргументах

    # Act (Действие)
    main()

    # Assert (Проверка)
    # Проверяем, что контейнер был создан
    mock_container.assert_called_once()
    # Проверяем, что из контейнера был получен коннектор
    mock_container().connector.assert_called_once()
    # Проверяем, что был запущен метод run() коннектора
    mock_asyncio_run.assert_called_once_with(mock_container().connector().run())
