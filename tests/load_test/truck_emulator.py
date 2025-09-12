import json
import time
import paho.mqtt.client as mqtt
import uuid
import random
import math

RADIUS_KM = 5 # Радиус кругового движения в километрах

class TruckEmulator:
    def __init__(self, device_id: str, center_latitude: float, center_longitude: float, speed: float, course: float, mqtt_broker_host: str, mqtt_broker_port: int):
        self.device_id = device_id
        self.center_latitude = center_latitude
        self.center_longitude = center_longitude
        self.speed = speed
        self.course = course
        self.mqtt_broker_host = mqtt_broker_host
        self.mqtt_broker_port = mqtt_broker_port
        self.client = self._setup_mqtt_client()
        self.angle_degrees = random.uniform(0, 360) # Начальный угол для каждого эмулятора
        self.latitude = 0.0 # Будет обновлено в generate_telemetry_message
        self.longitude = 0.0 # Будет обновлено в generate_telemetry_message

    def _setup_mqtt_client(self):
        client = mqtt.Client(client_id=f"truck_emulator_{self.device_id}")
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.connect(self.mqtt_broker_host, self.mqtt_broker_port, 60)
        client.loop_start()
        return client

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Эмулятор грузовика {self.device_id}: Подключен к MQTT брокеру!")
        else:
            print(f"Эмулятор грузовика {self.device_id}: Не удалось подключиться, код ошибки {rc}")

    def _on_disconnect(self, client, userdata, rc):
        print(f"Эмулятор грузовика {self.device_id}: Отключен от MQTT брокера с кодом {rc}")

    def generate_telemetry_message(self) -> dict:
        # Обновляем угол для движения по кругу
        self.angle_degrees = (self.angle_degrees + 1) % 360 # Изменяем угол на 1 градус за шаг
        angle_radians = math.radians(self.angle_degrees)

        # Приблизительное количество километров на градус широты и долготы
        # 1 градус широты ~ 111.32 км
        # 1 градус долготы ~ 111.32 * cos(широта) км
        lat_km_per_deg = 111.32
        lon_km_per_deg = 111.32 * math.cos(math.radians(self.center_latitude))

        # Вычисляем новые координаты
        self.latitude = self.center_latitude + (RADIUS_KM / lat_km_per_deg) * math.cos(angle_radians)
        self.longitude = self.center_longitude + (RADIUS_KM / lon_km_per_deg) * math.sin(angle_radians)

        message = {
            "device_id": self.device_id,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed + random.uniform(-1, 1), 2),
            "course": round(self.course + random.uniform(-5, 5), 2),
            "timestamp": int(time.time() * 1000)
        }
        return message

    def send_telemetry_message(self, topic: str):
        message = self.generate_telemetry_message()
        payload = json.dumps(message)
        self.client.publish(topic, payload)
        print(f"Эмулятор грузовика {self.device_id}: Отправлено сообщение в топик {topic}: {payload}")

    def run_loop(self, topic: str, interval: int = 1):
        try:
            while True:
                self.send_telemetry_message(topic)
                time.sleep(interval)
        except KeyboardInterrupt:
            print(f"Эмулятор грузовика {self.device_id}: Остановка эмуляции.")
        finally:
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    # Пример использования
    MQTT_BROKER_HOST = "localhost"  # Замените на ваш хост MQTT брокера
    MQTT_BROKER_PORT = 1883        # Замените на ваш порт MQTT брокера
    MQTT_TOPIC = "telemetry/trucks" # Замените на ваш топик

    emulator = TruckEmulator(
        device_id=str(uuid.uuid4()),
        latitude=55.7558,
        longitude=37.6173,
        speed=60.0,
        course=90.0,
        mqtt_broker_host=MQTT_BROKER_HOST,
        mqtt_broker_port=MQTT_BROKER_PORT
    )
    emulator.run_loop(MQTT_TOPIC, interval=1)
