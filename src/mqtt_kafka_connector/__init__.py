import os
import sys
from logging import config
from pathlib import Path

from dotenv import load_dotenv

from mqtt_kafka_connector.conf import LOGGING

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if "src" not in sys.path:
    sys.path.append(str(os.path.join(BASE_DIR, "src")))
#


config.dictConfig(LOGGING)
