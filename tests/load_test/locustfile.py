import uuid

from locust import User, between, task
from truck_emulator import TruckEmulator


class MqttTruckUser(User):
    wait_time = between(1, 2)  # Время ожидания между задачами

    def on_start(self):
        # Инициализация эмулятора грузовика для каждого пользователя Locust
        self.device_id = str(uuid.uuid4())
        self.emulator = TruckEmulator(
            device_id=self.device_id,
            center_latitude=55.7558,
            center_longitude=37.6173,
            speed=60.0,
            course=90.0,
            mqtt_broker_host="localhost",  # Предполагается, что брокер запущен локально
            mqtt_broker_port=1883,
        )

    @task
    def send_telemetry(self):
        # Отправка сообщения телеметрии
        topic = "telemetry/trucks"
        self.emulator.send_telemetry_message(topic)

    def on_stop(self):
        # Остановка MQTT клиента при завершении работы пользователя
        self.emulator.client.loop_stop()
        self.emulator.client.disconnect()
