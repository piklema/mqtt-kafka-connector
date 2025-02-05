from mqtt_kafka_connector.clients.base_http import BaseHTTPClient
import time
from mqtt_kafka_connector import conf


class RetentionCache:
    """Хранение данных с заданным временем жизни.

    При добавлении данных устанавливается время их добавления, а при получении
    данных проверяется, не устарели ли они. Если данные устарели, то они
    удаляются из кэша.
    """

    def __init__(self, default_retention: float):
        self.default_retention: float = default_retention
        self.last_cleanup: float = time.time()
        self.cache: dict[int, dict] = {}
        self.retention: dict[int, float] = {}

    def now(self) -> float:
        return time.time()

    def cleanup(self) -> None:
        if self.last_cleanup + self.default_retention < self.now():
            return

        for key in list(self.cache.keys()):
            if self.now() - self.retention[key] >= self.default_retention:
                del self.cache[key]
                del self.retention[key]

    def __setitem__(self, key: int, value: dict) -> None:
        self.cache[key] = value
        self.retention[key] = self.now()
        self.cleanup()

    def __getitem__(self, key: int) -> dict:
        self.cleanup()
        return self.cache[key]


class SchemaRegistry:
    """
    Класс для работы с реестром схем.

    Атрибуты:
        cache (RetentionCache): Кэш для хранения схем.

    Методы:
        get_url(schema_id: int) -> str: Возвращает URL для получения схемы по идентификатору.
        get_schema(schema_id: int) -> dict: Возвращает схему по идентификатору.
    """

    def __init__(self):
        self.cache = RetentionCache(default_retention=24 * 60 * 60)  # 1 день
        self.client = BaseHTTPClient(headers={})

    def get_url(self, schemd_id: int) -> str:
        if host := conf.SCHEMA_REGISTRY_HOST:
            return f'{host}/subjects/{schemd_id}'
        raise Exception('Неверно сконфигурирована SCHEMA_REGISTRY_HOST')

    async def get_schema(self, schema_id: int) -> dict:
        """
        Возвращает схему по идентификатору.

        Аргументы:
            schema_id (int): Идентификатор схемы.

        Возвращает:
            dict: Схема.

        Исключения:
            KeyError: Если схема не найдена в кэше.
        """
        try:
            return self.cache[schema_id]
        except KeyError:
            url = self.get_url(schema_id)
            response = await self.client.get(url)
            if response.status == 200:
                self.cache[schema_id] = response.json()
                return self.cache[schema_id]
            raise Exception(
                f'Не удалось получить схему по идентификатору {schema_id}'
            )
