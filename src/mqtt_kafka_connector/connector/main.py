import asyncio
import logging
import signal
import sys

from mqtt_kafka_connector.container import Container

logger = logging.getLogger(__name__)


async def main_async():
    """
    Асинхронная основная функция, которая настраивает и запускает коннектор.
    """
    container = Container()
    connector = container.connector()

    loop = asyncio.get_running_loop()
    main_task = loop.create_task(connector.run())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: main_task.cancel())

    logger.info("Приложение запущено.")
    await main_task


def main():
    """
    Основная функция запуска приложения.
    """
    try:
        asyncio.run(main_async())
    except asyncio.CancelledError:
        logger.info("Приложение остановлено.")


if __name__ == "__main__":
    sys.exit(main())
