__author__ = 'smackware'

from typing import *

from collections import namedtuple

if TYPE_CHECKING:
    from rdisq.consts import ServiceUid

RequestPayload = namedtuple("RequestPayload", "task_id timeout args kwargs")

class SessionResult(NamedTuple):
    result: Any
    session_data: Dict

class ResponsePayload(NamedTuple):
    returned_value: Any
    raised_exception: Exception
    processing_time_seconds: float
    service_uid: "ServiceUid"
    session_data: Dict = None
