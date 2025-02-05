import argparse
import asyncio
from pathlib import Path

from confluent_kafka.schema_registry import (
    Schema,
    SchemaReference,
    SchemaRegistryClient,
)


async def reg():
    schema_registry_url = 'http://localhost:8081'
    sr = SchemaRegistryClient({'url': schema_registry_url})
    path = Path('src/mqtt_kafka_connector/clients/schemas')

    schema = Schema((path / 'telemetry.avsc').read_text())
    # tel_schema_id = sr.register_schema(
    #     subject_name='telemetry-value',
    #     schema=schema,
    #     normalize_schemas=True,
    # )
    rs = sr.get_latest_version(subject_name='telemetry-value')

    refs = [
        SchemaReference(
            name='com.piklema.schemas.Telemetry',
            subject='telemetry-value',
            version=rs.version,
        )
    ]
    schema = Schema((path / 'batch.avsc').read_text(), references=refs)
    # sr.register_schema(
    #     subject_name='batch-value',
    #     schema=schema,
    #     normalize_schemas=True,
    # )


parser = argparse.ArgumentParser()
parser.add_argument('reg', help='зарегать схемы')
args = parser.parse_args()
if args.reg:
    asyncio.run(reg())
