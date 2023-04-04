import argparse
import asyncio
import csv
import datetime as dt
import logging
import time
from dataclasses import dataclass
from typing import Iterator, TextIO

import asyncio_mqtt as aiomqtt
from dataclasses_avroschema import AvroModel

from connector import conf

logger = logging.getLogger(__name__)


@dataclass
class ConfLine:
    vehicle_id_src: int
    vehicle_id_dst: int


@dataclass
class TruckTelemetry(AvroModel):
    time: dt.datetime
    object_id: int
    weight_dynamic: float
    accelerator_position: float
    height: float
    lat: float
    lon: float
    speed: float
    course: float


@dataclass
class TruckTelemetryList(AvroModel):
    data: list[TruckTelemetry]


def main():
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
        '-s',
        '--schema_id',
        type=int,
        help='schema_id',
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


def parse_conf_file(fp: TextIO) -> dict[int, ConfLine]:
    conf_dict: dict[int, ConfLine] = {}
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
            vehicle_id_src=vehicle_id_src,
            vehicle_id_dst=int(vehicle_id_dst),
        )
    return conf_dict


async def send_test_data(
    csv_data_filename: TextIO,
    conf_dict: dict[int, ConfLine],
    args: argparse.Namespace,
    infinite: bool = True,
):
    async with aiomqtt.Client(
        hostname=conf.MQTT_HOST,
        port=conf.MQTT_PORT,
        username=conf.MQTT_USER,
        password=conf.MQTT_PASSWORD,
        client_id=conf.MQTT_CLIENT_ID,
        clean_session=False,
    ) as client:
        while True:
            tt_gen = read_telemetry_data(csv_data_filename, conf_dict)

            tt_prev_time = None
            for tt in tt_gen:
                payload = tt.serialize()
                device_id = tt.object_id  # TODO
                topic = conf.MQTT_TOPIC_SOURCE_TEMPLATE.format(
                    customer_id=args.customer_id,
                    device_id=device_id,
                    schema_id=args.schema_id,
                )
                logger.debug('Publishing to %s', topic)
                await client.publish(topic, payload)

                period = tt.time - (tt_prev_time if tt_prev_time else tt.time)
                time.sleep(period.total_seconds())

                tt_prev_time = tt.time

            if args.infinite is False:
                break


async def send_to_kafka(client, tt: TruckTelemetry):
    display_bunches(tt)
    await client.publish()


def display_bunches(tt: TruckTelemetry) -> None:
    at = dt.datetime.now()
    print(at, tt)


def read_telemetry_data(
    fp: TextIO, conf_dict: dict[int, ConfLine]
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
        object_id_dst = conf_dict[object_id].vehicle_id_dst
        truck_telemetry = TruckTelemetry(
            time=time,
            object_id=object_id_dst,
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
