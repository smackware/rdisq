from threading import Thread

import pytest
from redis import Redis

from rdisq.request.request import Request, MultiRequest
from rdisq.request.message import RdisqMessage, MessageRequestData
from rdisq.request.dispatcher import RequestDispatcher
from rdisq.request.receiver import (
    ReceiverService, StartHandling, StopHandling, GetRegisteredMessages,
    CORE_RECEIVER_MESSAGES, AddQueue, RemoveQueue)
from rdisq.response import RdisqResponseTimeout
from tests._other_module import MessageFromExternalModule


class AddMessageRequestData(MessageRequestData):
    def __init__(self, first: int, second: int):
        self.first, self.second = first, second
        super(AddMessageRequestData, self).__init__()


class SumMessage(RdisqMessage):
    def __init__(self, first: int, second: int):
        self.first = first
        self.second = second
        super(SumMessage, self).__init__()


@pytest.fixture
def flush_redis():
    redis = Redis(host='127.0.0.1', port=6379, db=0)
    redis.flushdb()


@SumMessage.set_handler
def sum_(message: SumMessage):
    return message.first + message.second


def test_message(flush_redis):
    receiver_service = ReceiverService(message_class=SumMessage)

    call = Request(SumMessage(1, 2)).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait(1) == 1 + 2

    message = SumMessage(3, 2)
    call = Request(message).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait(1) == sum_(message)

    receiver_service.stop()


# =============================================


class AddMessage(RdisqMessage):
    def __init__(self, new: int) -> None:
        self.new = new
        super().__init__()


class SubtractMessage(RdisqMessage):
    def __init__(self, subtrahend: int) -> None:
        self.subtrahend = subtrahend
        super().__init__()


class Summer:
    def __init__(self, start: int = 0):
        self.sum = start

    @AddMessage.set_handler
    def add(self, message: AddMessage):
        self.sum += message.new
        return self.sum

    @SubtractMessage.set_handler
    def subtract(self, message: SubtractMessage):
        self.sum -= message.subtrahend
        return self.sum


def test_class_message(flush_redis):
    summer = Summer()
    receiver_service = ReceiverService(message_class=AddMessage, instance=summer)
    Thread(group=None, target=receiver_service.process).start()

    request = Request(AddMessage(1))
    request.send_async()
    assert request.wait(1) == 1

    try:
        request.send_and_wait_reply()
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed message reuse")

    request = Request(AddMessage(2))
    try:
        assert request.response
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed getting result before the evnet has run")

    assert request.send_and_wait_reply() == 3

    assert summer.sum == 3
    assert request.response.returned_value == 3
    receiver_service.stop()


def test_dynamic_service(flush_redis):
    summer = Summer()
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    receiver_service.register_message(StartHandling(SumMessage))
    assert Request(SumMessage(1, 2)).send_and_wait_reply() == 3

    receiver_service.unregister_message(StopHandling(SumMessage))

    try:
        Request(SumMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    try:
        Request(AddMessage(1)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    receiver_service.register_message(StartHandling(AddMessage, summer))
    Request(AddMessage(1)).send_and_wait_reply()
    Request(AddMessage(2)).send_and_wait_reply()
    assert summer.sum == 3

    receiver_service.stop()


def test_service_control_messages(flush_redis):
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    assert Request(StartHandling(SumMessage)).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    try:
        Request(StartHandling(SumMessage)).send_and_wait_reply()
    except Exception:
        pass
    else:
        raise RuntimeError("Should have failed to re-sum_ a message to a receiver.")

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    receivers_from_redis = dispatcher.get_receiver_services()
    assert receivers_from_redis[receiver_service.uid].registered_messages == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert receivers_from_redis[receiver_service.uid].uid == receiver_service.uid

    assert receiver_service.get_registered_messages() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert Request(GetRegisteredMessages()).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert Request(SumMessage(1, 2)).send_and_wait_reply() == 3
    Request(StopHandling(SumMessage)).send_and_wait_reply()

    try:
        Request(SumMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    assert Request(StartHandling(AddMessage, {"start": 1})).send_and_wait_reply() == {
        AddMessage} | CORE_RECEIVER_MESSAGES
    assert Request(AddMessage(3)).send_and_wait_reply() == 4

    receiver_service.stop()


def test_queues(flush_redis):
    receiver_service = ReceiverService()

    Request(AddQueue(new_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' in receiver_service.listening_queues

    Request(StartHandling(SumMessage)).send_async()
    receiver_service.rdisq_process_one()

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    r = dispatcher.queue_task("ReceiverService_test_queue", SumMessage(1, 2))
    receiver_service.rdisq_process_one()
    assert r.wait(1) == 3

    Request(RemoveQueue(old_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' not in receiver_service.listening_queues


def test_multi(flush_redis):
    receiver_service_1 = ReceiverService()
    receiver_service_2 = ReceiverService()

    request = MultiRequest(StartHandling(SumMessage)).send_async()
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    r = request.wait(1)
    assert r == [{SumMessage} | CORE_RECEIVER_MESSAGES, {SumMessage} | CORE_RECEIVER_MESSAGES]

    request = MultiRequest(SumMessage(1, 3)).send_async()
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    r = request.wait(1)
    assert r == [4, 4]

    request = MultiRequest(
        SumMessage(4, 4),
        lambda s: s.uid == receiver_service_1.uid).send_async()
    receiver_service_1.rdisq_process_one()
    r = request.wait(1)
    assert r == [8]

    receiver_service_3 = ReceiverService()
    request = MultiRequest(
        SumMessage(4, 4),
        lambda s: s.uid in [receiver_service_1.uid, receiver_service_2.uid]).send_async()

    assert receiver_service_3.rdisq_process_one(1) is False
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    assert request.wait(1) == [8, 8]


class _C:
    @MessageFromExternalModule.set_handler
    def f(self, m: MessageFromExternalModule):
        pass


def test_get_handler_class(flush_redis):
    receiver_service_1 = ReceiverService()
    assert AddMessage.handler_factory._handler_class == Summer
    assert StartHandling.handler_factory._handler_class == type(receiver_service_1)
    assert MessageFromExternalModule.handler_factory._handler_class == _C
    assert SumMessage.handler_factory._handler_class is None


def test_handler_class_reuse(flush_redis):
    receiver_service_1 = ReceiverService()
    Request(StartHandling(AddMessage, {})).send_async()
    receiver_service_1.rdisq_process_one()
    Request(StartHandling(SubtractMessage)).send_async()
    receiver_service_1.rdisq_process_one()

    r = Request(AddMessage(4)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 4

    r = Request(SubtractMessage(3)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 1
