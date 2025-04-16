import sys
from logging import config
from pathlib import Path

from dotenv import load_dotenv

from mqtt_kafka_connector.conf import LOGGING

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent
src_path = str(BASE_DIR / "src")
if src_path not in sys.path:
    sys.path.append(src_path)


config.dictConfig(LOGGING)
