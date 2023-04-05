from mqtt_kafka_connector.utils import Template


def test_template_topic():
    t = Template('customer/{customer_id}/dev/{device_id}/v{schema_id}')
    res = t.to_dict('not_match')
    assert res is None

    res = t.to_dict('customer/my_customer_id/dev/my_device_id/v1')
    assert res == dict(
        customer_id='my_customer_id',
        device_id='my_device_id',
        schema_id='1',
    )

    res = t.to_topic()
    assert res == 'customer/+/dev/+/v+'
