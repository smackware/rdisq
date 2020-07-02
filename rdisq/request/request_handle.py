import time
from abc import abstractmethod
from typing import *

from rdisq.request.message import RdisqMessage
from rdisq.request.dispatcher import ReceiverServiceStatus, RequestDispatcher
from rdisq.consts import QueueName, ServiceUid
from rdisq.request.receiver import AddQueue

if TYPE_CHECKING:
    from rdisq.response import RdisqResponse


class BaseRequestPlurality:
    return_type: List["RdisqResponse"] = List["RdisqResponse"]


class BaseRequestHandle:
    dispatcher: ClassVar[RequestDispatcher] = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    _target_service_uids: Set[ServiceUid]

    def __init__(self, message: RdisqMessage,
                 service_filter: Callable[["ReceiverServiceStatus"], bool] = None,
                 targets: Set[ServiceUid] = None
                 ):
        """
        :param message:
        :param service_filter: Filter which services will be targeted by this request.
        :param targets: List of service UIDs this request is aimed at.
        """
        if service_filter is not None and targets is not None:
            raise RuntimeError("Can't provide both a filter and a target-list")

        self._target_service_uids = targets
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

    def wait(self, timeout=None):
        raise NotImplementedError

    def send_and_wait_reply(self, timeout=None) -> Any:
        """
        Send the message and wait for a reply.
        :param timeout: How long to wait for reply .
        :return: The returned value of the remote method.
        """
        self.send_async()
        return self.wait(timeout)

    def _get_target_uids(self) -> Set[ServiceUid]:
        if not self._target_service_uids:
            services = self.dispatcher.filter_services(self._service_filter)
            service_ids = map(lambda s: s.uid, services)
            self._target_service_uids = set(service_ids)
        return self._target_service_uids

    def _get_queue_for_services(self, service_uids: Set[str]) -> QueueName:
        preexisting = self.dispatcher.find_queues_for_services(service_uids)
        if preexisting:
            queue = list(preexisting)[0]
        else:
            queue = ''
            # queue = self.dispatcher.generate_queue_name()
            # MultiRequestHandle(
            #     AddQueue(queue),
            #     targets=self._get_target_uids()).send_and_wait_reply()

        return queue

    def _automatic_filter(self):
        message_class = type(self.message)

        def filter_by_message(service_status: ReceiverServiceStatus) -> bool:
            return message_class in service_status.registered_messages

        self._service_filter = filter_by_message


class Request(BaseRequestHandle):
    _response: "RdisqResponse"

    @property
    def task_id(self):
        return self._response._task_id

    @property
    def response(self) -> "RdisqResponse":
        if self._sent:
            return self._response
        else:
            raise RuntimeError("not supposed to try to get results before the message was sent")

    def wait(self, timeout=None):
        return self._response.wait(timeout)

    def send_async(self) -> "Request":
        super(Request, self).send_async()
        self._response = self.dispatcher.queue_task(
            # f"{RECEIVER_SERVICE_NAME}_{self.message.get_message_class_id()}",
            self._get_channel(),
            self.message
        )

        return self

    def _get_channel(self) -> QueueName:
        channel = self._get_queue_for_services(self._get_target_uids())
        return channel


class MultiRequest(BaseRequestHandle):
    _targets: Set[ServiceUid]
    _requests: List[Request]

    def send_async(self) -> "MultiRequest":
        super(MultiRequest, self).send_async()
        target_uids = self._get_target_uids()
        self._requests = []
        for target_uid in target_uids:
            self._requests.append(
                Request(self.message, lambda s: s.uid == target_uid).
                    send_async())
        return self

    def wait(self, timeout=None):
        start = time.time()
        queues: List[QueueName] = [r.task_id for r in self._requests]
        reply_count = 0
        while reply_count < len(queues):
            queue_name, response = self.dispatcher.get_redis().brpop(queues, timeout)
            queue_name = queue_name.decode()
            for r in self._requests:
                if r.task_id == queue_name:
                    r.response.process_response(response)
                    reply_count += 1
                    break
        if reply_count < len(queues):
            raise RuntimeError(f"Timeout waiting for replies. "
                               f"Got {reply_count} out of {len(queues)}")
        else:
            return [r.response.response_payload.returned_value for
                    r in self._requests]
