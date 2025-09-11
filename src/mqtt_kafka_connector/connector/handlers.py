from __future__ import annotations

import io
import logging
from collections import defaultdict

import aiomqtt
import fastavro
import orjson
from aiomqtt.message import Message

from mqtt_kafka_connector.settings import settings
from mqtt_kafka_connector.context_vars import message_uuid_var, setup_context_vars
from mqtt_kafka_connector.utils import Template

logger = logging.getLogger(__name__)

KafkaHeadersType = list[tuple[str, bytes]]
TopicHeaders = tuple[str, bytes, KafkaHeadersType]


class BaseHandler:
    async def handle(self, message: Message) -> bool:
        raise NotImplementedError


class MessageHandler(BaseHandler):
    def __init__(self, kafka_producer: "KafkaProducer"):
        self.kafka_producer = kafka_producer

    def _setup_vars(self, message: Message, template: str) -> dict | None:
        mqtt_params = Template(template).to_dict(message.topic.value)
        device_id = mqtt_params.get("device_id")
        if not device_id:
            logger.error(
                "В параметрах топика %r не найден ID устройства", mqtt_params
            )
            return None

        setup_context_vars(device_id, mqtt_params.get("customer_id"))
        return mqtt_params

    @staticmethod
    def get_kafka_message_params(
        mqtt_topic_params: dict,
        topic_name_tpl: str,
    ) -> TopicHeaders:
        """
        Формирование параметров для отправки сообщения в Kafka.

        Args:
            mqtt_topic_params: Параметры, извлеченные из топика MQTT.
            topic_name_tpl: Шаблон для формирования имени топика Kafka.

        Returns:
            Кортеж с именем топика, ключом и заголовками для Kafka.
        """
        kafka_topic = topic_name_tpl.format(**mqtt_topic_params)
        kafka_key = settings.KAFKA_KEY_TEMPLATE.format(**mqtt_topic_params).encode()
        kafka_headers = [
            (k, v.encode())
            for k, v in mqtt_topic_params.items()
            if k in settings.KAFKA_HEADERS_LIST.split(",")
        ]

        if settings.WITH_MESSAGE_DESERIALIZE:
            kafka_headers.append(("message_deserialized", b"1"))

        if settings.TRACE_HEADER:
            kafka_headers.append(
                (settings.TRACE_HEADER, message_uuid_var.get().encode())
            )

        return kafka_topic, kafka_key, kafka_headers


class TelemetryHandler(MessageHandler):
    def __init__(
        self,
        kafka_producer: "KafkaProducer",
        schema_client: "SchemaClient",
        prometheus: "Prometheus" | None = None,
    ):
        super().__init__(kafka_producer)
        self.schema_client = schema_client
        self.prometheus = prometheus
        self.last_messages = defaultdict(dict)

    async def handle(self, mqtt_message: Message) -> bool:
        """
        Обработчик телеметрических сообщений.

        Args:
            mqtt_message: Сообщение из MQTT.

        Returns:
            True, если сообщение было успешно обработано, иначе False.
        """
        if not (mqtt_params := self._setup_vars(
            mqtt_message, settings.MQTT_TOPIC_SOURCE_TEMPLATE
        )):
            return False

        try:
            (
                kafka_topic,
                kafka_key,
                kafka_headers,
            ) = self.get_kafka_message_params(
                mqtt_params,
                settings.TELEMETRY_KAFKA_TOPIC,
            )
        except KeyError as err:
            logger.error(
                "Ключ не найден в параметрах топика %r: %s", mqtt_params, err
            )
            return False

        schema_id = int(dict(kafka_headers)["schema_id"])
        telemetry_msg_pack = await self.get_telemetry_message_pack(
            mqtt_message, schema_id
        )
        if self.check_telemetry_messages_pack(
            mqtt_message.topic.value, telemetry_msg_pack
        ):
            await self.kafka_handler(
                telemetry_msg_pack,
                kafka_topic,
                kafka_key,
                kafka_headers,
            )
            self.prometheus.messages_counter_add(value=len(telemetry_msg_pack))

        return True

    async def deserialize(self, msg: aiomqtt.Message, schema_id: int) -> dict:
        """
        Десериализация сообщения из Avro формата.

        Args:
            msg: Сообщение из MQTT.
            schema_id: ID схемы для десериализации.

        Returns:
            Десериализованное сообщение в виде словаря.
        """
        schema = await self.schema_client.get_schema(schema_id)

        if not schema:
            raise RuntimeError("Схема не найдена")

        fp = io.BytesIO(msg.payload)
        try:
            parsed_schema = fastavro.parse_schema(schema)
            data = fastavro.schemaless_reader(fp, parsed_schema)
        except (IndexError, StopIteration, EOFError) as e:
            error_message = (
                f"Сообщение не валидно: ошибка десериализации Avro: {e}"
            )
            logger.error(error_message, exc_info=True)
            raise RuntimeError(error_message) from e

        logger.debug("Сообщение десериализовано: data=%s", data)

        return data

    def check_telemetry_messages_pack(
        self, mqtt_topic: str, telemetry_msg_pack: list
    ) -> bool:
        """
        Проверка пакета телеметрических сообщений на дубликаты.

        Args:
            mqtt_topic: Топик MQTT, из которого пришло сообщение.
            telemetry_msg_pack: Пакет телеметрических сообщений.

        Returns:
            True, если пакет не является дубликатом, иначе False.
        """
        last_message = telemetry_msg_pack[-1]
        messages_count = len(telemetry_msg_pack)

        if self.last_messages[mqtt_topic] != last_message:
            self.last_messages[mqtt_topic] = last_message
        else:
            logger.info(
                "Пакет сообщений из %s уже отправляется. Пропуск отправки в kafka",
                mqtt_topic,
            )
            return False

        logger.info("Получено %s сообщений из %s", messages_count, mqtt_topic)
        return True

    async def get_telemetry_message_pack(
        self,
        mqtt_message: aiomqtt.Message,
        schema_id: int,
    ) -> list[dict] | None:
        """
        Получение и десериализация пакета телеметрических сообщений.

        Args:
            mqtt_message: Сообщение из MQTT.
            schema_id: ID схемы для десериализации.

        Returns:
            Список телеметрических сообщений или None, если пакет пустой.
        """
        mqtt_topic = mqtt_message.topic

        logger.debug(
            "Получено сообщение из "
            "mqtt_topic.value=%s message.payload=%s message.qos=%s",
            mqtt_topic.value,
            mqtt_message.payload,
            mqtt_message.qos,
        )

        if settings.WITH_MESSAGE_DESERIALIZE:
            mqtt_msg_dict = await self.deserialize(mqtt_message, schema_id)
            telemetry_msg_pack = mqtt_msg_dict["messages"]

        else:
            data = mqtt_message.payload
            telemetry_msg_pack = orjson.loads(data.decode())["messages"]

        if not telemetry_msg_pack:
            logger.warning("Пустой пакет сообщений")
            return

        return telemetry_msg_pack

    async def kafka_handler(
        self,
        messages: list,
        kafka_topic: str,
        kafka_key: bytes,
        kafka_headers: KafkaHeadersType,
    ):
        """
        Обработчик для отправки сообщений в Kafka.

        Args:
            messages: Список сообщений для отправки.
            kafka_topic: Имя топика Kafka.
            kafka_key: Ключ сообщения.
            kafka_headers: Заголовки сообщения.
        """
        logger.info(
            "Начало отправки в kafka topic=%s, key=%s", kafka_topic, int(kafka_key)
        )

        if settings.KAFKA_SEND_BATCHES:
            await self.kafka_producer.send_batch(
                kafka_topic,
                messages,
                kafka_key,
                kafka_headers,
            )

        else:
            for msg in messages:
                await self.kafka_producer.send(
                    kafka_topic,
                    message=msg,
                    key=kafka_key,
                    headers=kafka_headers,
                )

        return True


class FStateHandler(MessageHandler):
    async def handle(self, mqtt_message: Message) -> bool:
        """
        Обработчик сообщений о состоянии.

        Args:
            mqtt_message: Сообщение из MQTT.

        Returns:
            True, если сообщение было успешно обработано, иначе False.
        """
        logger.info(
            "Обработчик сообщений о состоянии mqtt_message topic=%r, payload=%r, qos=%r, retain=%r, mid=%r, properties=%r",
            mqtt_message.topic,
            mqtt_message.payload,
            mqtt_message.qos,
            mqtt_message.retain,
            mqtt_message.mid,
            mqtt_message.properties,
        )

        if not (mqtt_params := self._setup_vars(
            mqtt_message, settings.MQTT_FSTATE_SOURCE_TEMPLATE
        )):
            return False

        try:
            (
                kafka_topic,
                kafka_key,
                kafka_headers,
            ) = self.get_kafka_message_params(
                mqtt_params,
                settings.FSTATE_KAFKA_TOPIC,
            )
            message = orjson.loads(
                mqtt_message.payload.decode()
                if isinstance(mqtt_message.payload, bytes)
                else str(mqtt_message.payload)
            )
        except orjson.JSONDecodeError as err:
            logger.error("Ошибка декодирования JSON: %s", err)
            return False
        except KeyError as err:
            logger.error(
                "Ключ не найден в параметрах топика %r: %s", mqtt_params, err
            )
            return False

        await self.kafka_producer.send(
            kafka_topic,
            message=message,
            key=kafka_key,
            headers=kafka_headers,
        )

        return True


class TopicRouter:
    def __init__(
        self,
        telemetry_handler: TelemetryHandler,
        fstate_handler: FStateHandler,
    ):
        self.telemetry_handler = telemetry_handler
        self.fstate_handler = fstate_handler

    async def handle(self, mqtt_message: aiomqtt.Message) -> bool:
        if mqtt_message.topic.matches(settings.MQTT_TOPIC_SOURCE_MATCH):
            return await self.telemetry_handler.handle(mqtt_message)
        elif mqtt_message.topic.matches(settings.MQTT_FSTATE_SOURCE_MATCH):
            return await self.fstate_handler.handle(mqtt_message)
        else:
            logger.warning("Неизвестный топик %s", mqtt_message.topic)
            return False