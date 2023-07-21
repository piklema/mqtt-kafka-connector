import uuid
from contextvars import ContextVar

MESSAGE_UUID = 'message_uuid'

message_uuid_var = ContextVar(MESSAGE_UUID, default=uuid.uuid4().hex)
