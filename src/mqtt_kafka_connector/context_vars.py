import uuid
from contextvars import ContextVar

from sentry_sdk import set_tag

MESSAGE_UUID = 'message_uuid'
DEVICE_ID = 'device_id'

message_uuid_var = ContextVar(MESSAGE_UUID, default='')
device_id_var = ContextVar(DEVICE_ID, default=0)


def setup_context_vars(device_id: int):
    uuid_hex = uuid.uuid4().hex
    message_uuid_var.set(uuid_hex)
    device_id_var.set(device_id)
    set_tag(DEVICE_ID, device_id)
    set_tag(MESSAGE_UUID, uuid_hex)
