def prepare_topic_mqtt_to_kafka(mqtt_topic: str) -> str:
    customer_id = mqtt_topic.split('/')[1]
    return f'customer_{customer_id}'
