FROM python:3.11.0-alpine

COPY . /app
WORKDIR /app

RUN pip install -e .

ENTRYPOINT ["mqtt_kafka_connector"]
