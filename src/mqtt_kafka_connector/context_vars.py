import uuid
from contextvars import ContextVar
from sentry_sdk import set_tag

MESSAGE_UUID = 'message_uuid'
DEVICE_ID = 'device_id'

message_uuid_var = ContextVar(MESSAGE_UUID)
device_id_var = ContextVar(DEVICE_ID)


def setup_vars(device_id: int):
    message_uuid_var.set(uuid.uuid4().hex)
    device_id_var.set(device_id)
    set_tag(DEVICE_ID, device_id)
