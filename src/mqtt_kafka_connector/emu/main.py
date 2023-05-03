import argparse
import asyncio
import csv
import datetime as dt
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Dict, Iterator, TextIO

from mqtt_kafka_connector import conf
from mqtt_kafka_connector.connector import Connector

logger = logging.getLogger(__name__)


@dataclass
class ConfLine:
    object_id: int
    device_id: int


@dataclass
class TruckTelemetry:
    time: dt.datetime
    device_id: int
    weight_dynamic: float
    accelerator_position: float
    height: float
    lat: float
    lon: float
    speed: float
    course: float


class DateTimeEncoder(json.JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (dt.date, dt.datetime)):
            aware_dt = obj.replace(tzinfo=dt.timezone.utc)
            return aware_dt.isoformat()


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description='Send test data to Kafka')
    parser.add_argument(
        'config_filename',
        type=argparse.FileType('r'),
        help='File with configuration',
    )
    parser.add_argument(
        'csv_data_filename',
        type=argparse.FileType('r'),
        help='CSV file with data',
        default='',
    )
    parser.add_argument(
        '-c',
        '--customer_id',
        type=int,
        help='customer_id',
    )
    parser.add_argument(
        '-i',
        '--infinite',
        action='store_true',
        help='infinite loop',
    )
    args = parser.parse_args()
    conf_dict = parse_conf_file(args.config_filename)
    logger.info('Start emulating data')
    asyncio.run(send_test_data(args.csv_data_filename, conf_dict, args))


def parse_conf_file(fp: TextIO) -> Dict[int, ConfLine]:
    conf_dict: Dict[int, ConfLine] = {}
    for line in fp:
        line = line.strip()
        if not line:
            continue
        if line.startswith('#'):
            continue
        try:
            vehicle_id_src, vehicle_id_dst = line.split(',')
        except ValueError:
            logger.error('Invalid line: %s', line)
            continue
        vehicle_id_src = int(vehicle_id_src)
        conf_dict[vehicle_id_src] = ConfLine(
            object_id=vehicle_id_src,
            device_id=int(vehicle_id_dst),
        )
    return conf_dict


async def send_test_data(
    csv_data_filename: TextIO,
    conf_dict: Dict[int, ConfLine],
    args: argparse.Namespace,
):
    kafka_topic = conf.KAFKA_TOPIC_TEMPLATE.format(
        customer_id=args.customer_id
    )
    conn = Connector(message_deserialize=False)
    while True:
        telemetry_gen = read_telemetry_data(csv_data_filename, conf_dict)

        telemetry_prev_time = None
        for telemetry in telemetry_gen:
            d = asdict(telemetry)
            data = json.dumps(d, cls=DateTimeEncoder).encode()
            kafka_headers = [
                ('message_deserialized', b'1'),
            ]
            logger.debug('Publishing to %s', kafka_topic)
            await conn.send_to_kafka(
                kafka_topic,
                data,
                key=str(telemetry.device_id).encode(),
                headers=kafka_headers,
            )

            period = telemetry.time - (
                telemetry_prev_time if telemetry_prev_time else telemetry.time
            )
            time.sleep(period.total_seconds())

            telemetry_prev_time = telemetry.time

        if args.infinite is False:
            break


def read_telemetry_data(
    fp: TextIO, conf_dict: Dict[int, ConfLine]
) -> Iterator[TruckTelemetry]:
    csv_reader = csv.DictReader(fp)
    for row in csv_reader:
        time = dt.datetime.fromisoformat(row['time'])
        object_id = int(row['objectid'])
        weight_dynamic = float(row['weight_dynamic'])
        accelerator_position = float(row['accelerator_position'])
        height = float(row['height'])
        lat = float(row['lat'])
        lon = float(row['lon'])
        speed = float(row['speed'])
        course = float(row['course'])
        try:
            device_id = conf_dict[object_id].device_id
        except KeyError:
            continue
        truck_telemetry = TruckTelemetry(
            time=time,
            device_id=device_id,
            weight_dynamic=weight_dynamic,
            accelerator_position=accelerator_position,
            height=height,
            lat=lat,
            lon=lon,
            speed=speed,
            course=course,
        )
        yield truck_telemetry


if __name__ == '__main__':
    main()
