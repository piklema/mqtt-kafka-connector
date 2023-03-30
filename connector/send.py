from collections import defaultdict
import csv
import argparse
import logging
from typing import TextIO
from dataclasses import dataclass
import datetime as dt
import time

from connector.main import Connector

logger = logging.getLogger(__name__)


@dataclass
class ConfLine:
    """Para"""

    vehicle_id_src: int
    vehicle_id_dst: int


@dataclass
class TruckTelemetry:
    time: dt.datetime
    object_id: int
    weight_dynamic: float
    accelerator_position: float
    height: float
    lat: float
    lon: float
    speed: float
    course: float


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


def send_test_data():
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
    args = parser.parse_args()
    conf_dict = parse_conf_file(args.config_filename)
    start_at = time.time()
    print(f'Started at {start_at}')
    # Для каждого транспортного средства собираем данные, не более 10 штук,
    # либо не более 10 секунд, затем отправляем в кафку
    truck_bunch_data: defaultdict[int, list[TruckTelemetry]] = defaultdict(
        list
    )
    trucks = read_telemetry_data(args.csv_data_filename, conf_dict)
    for truck in trucks:
        bunch = truck_bunch_data[truck.object_id]
        display_bunches(truck_bunch_data)
        if len(bunch) >= 10 or check_timeout(truck, bunch):
            send_to_kafka(bunch)
            bunch = []
        bunch.append(truck)
        truck_bunch_data[truck.object_id] = bunch

    # connector = Connector()
    # connector.send_to_kafka(args.topic, args.message.encode('utf-8'))


def display_bunches(truck_bunch_data: defaultdict[int, list[TruckTelemetry]]):
    print(f'\nBunches: ----------------------------------------')
    for object_id, bunch in truck_bunch_data.items():
        progress = '=' * len(bunch) + f' {len(bunch)}'
        if len(bunch) < 1:
            period = 0
        else:
            period = (bunch[-1].time - bunch[0].time).total_seconds()
        print(f'{object_id} {period}s {progress}')
    print('\n\n')
    time.sleep(0.1)


def send_to_kafka(bunch: list[TruckTelemetry]):
    print(f'\nSENDING {len(bunch)} for OBJECT_ID {bunch[0].object_id}\n')
    time.sleep(1)


def check_timeout(truck: TruckTelemetry, bunch: list[TruckTelemetry]) -> bool:
    if not bunch:
        return False
    return (truck.time - bunch[0].time).total_seconds() > 10


def read_telemetry_data(fp: TextIO, conf_dict: dict[int, ConfLine]):
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
    send_test_data()
