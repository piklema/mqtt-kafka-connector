FROM python:3.11.0-alpine

ARG RELEASE_VERSION
ENV RELEASE_VERSION=${RELEASE_VERSION}

COPY . /app
WORKDIR /app


RUN pip install -e .

ENTRYPOINT ["mqtt_kafka_connector"]
