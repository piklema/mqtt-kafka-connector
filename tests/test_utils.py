from deepdiff import DeepDiff

from mqtt_kafka_connector.utils import Template, clean_empty_fields, clean_none_fields


def test_template_topic():
    template = Template('customer/{customer_id}/dev/{device_id}/v{schema_id}')
    res = template.to_dict('not_match')
    assert res == {}

    res = template.to_dict('customer/my_customer_id/dev/my_device_id/v1')
    assert res == dict(
        customer_id='my_customer_id',
        device_id='my_device_id',
        schema_id='1',
    )

    res = template.to_topic()
    assert res == 'customer/+/dev/+/v+'


async def test_clean_empty():
    test_list = [None, 1, 2, None]
    test_dict = {
        "a": 1,
        "b": 0,
        "c": 2,
        "e": None,
    }

    assert clean_empty_fields(test_list) == [1, 2]
    assert not DeepDiff(clean_empty_fields(test_dict), {"a": 1, "c": 2})


async def test_clean_none():
    test_list = [None, 1, 2, float('nan')]
    test_dict = {
        "a": 1,
        "b": 0,
        "c": 2,
        "e": None,
        "f": float('nan'),
    }

    assert clean_none_fields(test_list) == [1, 2]
    assert not DeepDiff(
        clean_none_fields(test_dict),
        {
            "a": 1,
            "b": 0,
            "c": 2,
        },
    )
