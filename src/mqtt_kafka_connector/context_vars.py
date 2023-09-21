import uuid
from contextvars import ContextVar

from sentry_sdk import set_tag

MESSAGE_UUID = 'message_uuid'
DEVICE_ID = 'device_id'

message_uuid_var = ContextVar(MESSAGE_UUID, default=uuid.uuid4().hex)
device_id_var = ContextVar(DEVICE_ID, default=0)


def setup_vars(device_id: int):
    device_id_var.set(device_id)
    set_tag(DEVICE_ID, device_id)
    set_tag(MESSAGE_UUID, message_uuid_var.get())
