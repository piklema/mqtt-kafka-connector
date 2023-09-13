# MQTT Kafka connector

### Build local image
```shell
docker build --no-cache -t piklema/mqtt-kafka-connector:latest .
```

### How to run
Rename `.env.example` to `.env` and changes values

### Run unittest
```shell
$ pip install -e ".[develop]"
$ make test
```

### Run script
```shell
$ mqtt_kafka_connector
```
