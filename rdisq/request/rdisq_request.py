from abc import abstractmethod
from typing import *

from rdisq.request.message import RdisqMessage
from rdisq.request.dispatcher import ReceiverServiceStatus, RequestDispatcher
from rdisq.consts import QueueName, ServiceUid
from rdisq.request.receiver import AddQueue

if TYPE_CHECKING:
    from rdisq.response import RdisqResponse


class BaseRequest:
    dispatcher: ClassVar[RequestDispatcher] = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    _target_service_uids: Optional[Set[ServiceUid]]
    _sent: bool = False
    _finished: bool = False

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
            self._filter_wrapper()

    @property
    def sent(self):
        return self._sent

    @property
    def finished(self):
        return self._finished

    @abstractmethod
    def send_async(self) -> "BaseRequest":
        if not self._get_target_uids():
            raise RuntimeError("Tried sending a request, but not suitable receiver services were found.")
        elif self._sent:
            raise RuntimeError("This message has already been sent")
        else:
            self._sent = True
            return self

    @abstractmethod
    def wait(self, timeout=None):
        if not self._sent:
            raise RuntimeError("Trying to wait on an un-sent request")

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

    def get_queue_for_services(self, service_uids: Set[str]) -> QueueName:
        preexisting = self.dispatcher.find_queues_for_services(service_uids)
        if not service_uids:
            raise RuntimeError("Got empty service_uids set")
        if preexisting:
            queue = list(preexisting)[0]
        else:
            queue = self.dispatcher.generate_queue_name()
            MultiRequest(
                AddQueue(queue),
                targets=self._get_target_uids()).send_and_wait_reply()

        return queue

    def _filter_wrapper(self, base_filter: Callable[[ReceiverServiceStatus], bool]=None):
        message_class = type(self.message)

        def filter_by_message(service_status: ReceiverServiceStatus) -> bool:
            flag = True
            if base_filter and not base_filter(service_status):
                flag = False
            if message_class not in service_status.registered_messages:
                flag = False
            return flag

        self._service_filter = filter_by_message


class RdisqRequest(BaseRequest):
    _response: "RdisqResponse"

    @property
    def returned_value(self):
        if not self.finished:
            raise RuntimeError("Tried getting returned value but the request hasn't finished yet.")
        else:
            return self._response.response_payload.returned_value

    @property
    def task_id(self):
        return self._response.task_id

    @property
    def response(self) -> "RdisqResponse":
        if self._sent:
            return self._response
        else:
            raise RuntimeError("not supposed to try to get results before the message was sent")

    def wait(self, timeout=None):
        super(RdisqRequest, self).wait()
        r = self._response.wait(timeout)
        self._finished = True
        return r

    def send_async(self) -> "RdisqRequest":
        super(RdisqRequest, self).send_async()
        self._response = self.dispatcher.queue_task(
            self.get_queue_for_services(self._get_target_uids()),
            self.message
        )

        return self


class MultiRequest(BaseRequest):
    _targets: Set[ServiceUid]
    _requests: List[RdisqRequest]

    def send_async(self) -> "MultiRequest":
        super(MultiRequest, self).send_async()
        self._requests = []
        for target_uid in self._get_target_uids():
            self._requests.append(
                RdisqRequest(self.message, lambda s: s.uid == target_uid).
                    send_async())
        return self

    def wait(self, timeout=None):
        super(MultiRequest, self).wait()
        queues: List[QueueName] = [r.task_id for r in self._requests]
        reply_count = 0
        while reply_count < len(queues):
            queue_name, response = self.dispatcher.get_redis().brpop(queues, timeout)
            queue_name = queue_name.decode()
            for r in self._requests:
                if r.task_id == queue_name:
                    r.response.process_response(response)
                    r._finished = True
                    reply_count += 1
                    break
        if reply_count < len(queues):
            self._finished = True
            raise RuntimeError(f"Timeout waiting for replies. "
                               f"Got {reply_count} out of {len(queues)}")
        else:
            self._finished = True
            return [r.returned_value for r in self._requests]
