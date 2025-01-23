# MQTT Kafka connector

## Local development
`uv venv -p 3.12`
`uv pip install --no-cache -r requirements.txt`

### Build local image
```shell
docker build --no-cache -t piklema/mqtt-kafka-connector:latest .
```

### How to run
Rename `.env.example` to `.env` and changes values

### Run unittest
```shell
$ uv pip install -e ".[develop]"
$ make test
```

### Run script
```shell
$ mqtt_kafka_connector
```


# Schema registry

[Use the Schema Registry API](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/)
[Pandaproxy Schema Registry](https://docs.redpanda.com/api/pandaproxy-schema-registry/)
[Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
