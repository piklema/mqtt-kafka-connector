FROM python:3.11.0-alpine

COPY . /app
WORKDIR /app

RUN apk update && apk add gcc \
                          libc-dev \
                          zlib-dev

RUN pip install --upgrade pip && \
    pip install -e .

ENTRYPOINT ["mqtt_kafka_connector"]
