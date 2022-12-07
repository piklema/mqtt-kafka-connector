from connector.utils import Template


def test_template_topic():
    t = Template(
        'customer/{customer_id}/dev/{device_id}/v{schema_version}',
        'customer_{customer_id}',
    )
    res = t.transform('not_match')
    assert res is None

    res = t.transform('customer/my_customer_id/dev/my_device_id/v1')
    assert res == 'customer_my_customer_id'


def test_template_header():
    t = Template(
        'customer/{customer_id}/dev/{device_id}/v{schema_version}',
        'schema_version:{schema_version}|device_id:{device_id}',
    )

    res = t.transform('customer/my_customer_id/dev/my_device_id/v1')
    assert res == 'schema_version:1|device_id:my_device_id'
