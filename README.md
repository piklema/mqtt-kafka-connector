# MQTT Kafka connector

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

### Run send test message command
```shell
send_test_message connector/test.cfg data/sorted_emulation_file.csv
```
You need to preprocess data file before send it to kafka.
```shell
# Remove the first line and save it to a variable
header=$(head -n 1 data/emulation_file.csv)
sed '1d' data/emulation_file.csv | sort -t ',' -k1 | (echo $header && cat) > data/sorted_emulation_file.csv
```
Sample test.cfg
```csv
# object_id, device_id
102, 1
...
```
