import asyncio
import sys

from mqtt_kafka_connector.container import Container


def main():
    """
    Основная функция запуска приложения.
    """
    container = Container()
    connector = container.connector()
    asyncio.run(connector.run())


if __name__ == "__main__":
    sys.exit(main())
