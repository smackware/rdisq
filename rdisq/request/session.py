from typing import *

import uuid

from rdisq.consts import RdisqSessionUid, ServiceUid
from rdisq.request.message import RdisqMessage
from rdisq.request.rdisq_request import RdisqRequest

if TYPE_CHECKING:
    from rdisq.request.dispatcher import ReceiverServiceStatus


class _RdisqSessionData:
    pass


class RdisqSession:
    _session_id: RdisqSessionUid
    _service_id: ServiceUid = None
    _service_filter: Callable[["ReceiverServiceStatus"], bool]
    _request: Optional[RdisqRequest]
    session_data: Dict = None

    def __init__(self, filter_: Callable[["ReceiverServiceStatus"], bool] = None):
        self._session_id = RdisqSessionUid(f"rdisq_session_{uuid.uuid4()}")
        self._request = None
        self._service_filter = filter_
        self.session_data = {}

    @property
    def current_request(self) -> RdisqRequest:
        return self._request

    def send(self, message: RdisqMessage):
        if self._request and not self._request.finished:
            raise RuntimeError("Previous request isn't done yet.")
        else:
            message.session_data = self.session_data
            if self._service_id:
                self._request = RdisqRequest(message, targets={self._service_id})
            else:
                self._request = RdisqRequest(message, service_filter=self._service_filter)
            self._request.send_async()

    def send_and_wait(self, message: RdisqMessage, timeout: int = None):
        self.send(message)
        return self.wait(timeout)

    def wait(self, timeout: int = None):
        if not self._request or not self._request.sent:
            raise RuntimeError("Tried waiting on a request, but there's no pending request to wait on.")
        else:
            r = self._request.wait(timeout)
            if self._request.response.response_payload.session_data is not None:
                self.session_data = self._request.response.response_payload.session_data
            if not self._service_id:
                self._service_id = self._request.response.response_payload.service_uid
            return r
