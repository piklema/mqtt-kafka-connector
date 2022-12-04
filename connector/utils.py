def prepare_topic_mqtt_to_kafka(mqtt_topic: str) -> str:
    try:
        customer_id = mqtt_topic.split('/')[1]
    except IndexError:
        return ''
    return f'customer_{customer_id}'
