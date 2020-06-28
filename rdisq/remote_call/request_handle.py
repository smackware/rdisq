from abc import abstractmethod
from functools import partial
from typing import *

from rdisq.remote_call.consts import RECEIVER_SERVICE_NAME
from rdisq.remote_call.message import RdisqMessage
from rdisq.remote_call.receiver import ReceiverService
from rdisq.remote_call.message_dispatcher import ReceiverServiceStatus, MessageDispatcher, QueueName

if TYPE_CHECKING:
    from rdisq.response import RdisqResponse


class BaseRequestPlurality:
    return_type: List["RdisqResponse"] = List["RdisqResponse"]


# ServiceFilter = TypeVar("ServiceFilter", Callable[["ReceiverServiceStatus"], bool])


class BaseRequestHandle:
    dispatcher: ClassVar[MessageDispatcher] = MessageDispatcher(host='127.0.0.1', port=6379, db=0)

    def __init__(self, message: RdisqMessage, service_filter: Callable[["ReceiverServiceStatus"], bool] = None):
        self._sent: bool = False
        self.message: RdisqMessage = message
        if service_filter:
            self._service_filter = service_filter
        else:
            self._automatic_filter()

    @abstractmethod
    def send_async(self) -> "BaseRequestHandle":
        if self._sent:
            raise RuntimeError("This message has already been sent")
        self._sent = True
        return self

    def wait(self):
        pass

    def _automatic_filter(self):
        message_class = type(self.message)
        def filter_by_message(service_status: ReceiverServiceStatus) -> bool:
            return message_class in service_status.registered_messages
        self._service_filter = filter_by_message


class RequestHandle(BaseRequestHandle):
    _response: "RdisqResponse"

    @property
    def response(self) -> "RdisqResponse":
        if self._sent:
            return self._response
        else:
            raise RuntimeError("not supposed to try to get results before the message was sent")

    def wait(self, timeout=None):
        return self._response.wait(timeout)

    def send_and_wait_reply(self, timeout=None) -> Any:
        """
        Send the message and wait for a reply.
        :param timeout: How long to wait for reply .
        :return: The returned value of the remote method.
        """
        self.send_async()
        return self._response.wait(timeout)

    def send_async(self) -> "RequestHandle":

        super(RequestHandle, self).send_async()
        self._response = self.dispatcher.queue_task(
            # f"{RECEIVER_SERVICE_NAME}_{self.message.get_message_class_id()}",
            self._get_channel(),
            self.message
        )

        return self

    def _get_channel(self) -> QueueName:
        services = self.dispatcher.filter_services(self._service_filter)
        service_ids = map(lambda s: s.uid, services)

        channel = self.dispatcher.get_queue_for_services(set(service_ids))
        return channel