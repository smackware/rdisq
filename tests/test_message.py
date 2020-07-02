from threading import Thread

import pytest
from redis import Redis

from rdisq.request.request_handle import RequestHandle, MultiRequestHandle
from rdisq.request.message import RdisqMessage, MessageRequestData
from rdisq.request.dispatcher import RequestDispatcher
from rdisq.request.receiver import (
    ReceiverService, StartHandling, StopHandling, GetRegisteredMessages,
    CORE_RECEIVER_MESSAGES, AddQueue, RemoveQueue)
from rdisq.response import RdisqResponseTimeout


class AddMessageRequestData(MessageRequestData):
    def __init__(self, first: int, second: int):
        self.first, self.second = first, second
        super(AddMessageRequestData, self).__init__()


class AddMessage(RdisqMessage):
    def __init__(self, first: int, second: int):
        self.first = first
        self.second = second
        super(AddMessage, self).__init__()


@pytest.fixture
def flush_redis():
    redis = Redis(host='127.0.0.1', port=6379, db=0)
    redis.flushdb()


@AddMessage.set_handler
def add(message: AddMessage):
    return message.first + message.second


def test_message(flush_redis):
    receiver_service = ReceiverService(message_class=AddMessage)

    call = RequestHandle(AddMessage(1, 2)).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait() == 1 + 2

    message = AddMessage(3, 2)
    call = RequestHandle(message).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait() == add(message)

    receiver_service.stop()


# =============================================


class SumMessage(RdisqMessage):
    def __init__(self, new: int) -> None:
        self.new = new
        super().__init__()


class Summer:
    def __init__(self, start: int = 0):
        self.sum = start

    @SumMessage.set_handler
    def add(self, message: SumMessage):
        self.sum += message.new
        return self.sum


def test_class_message(flush_redis):
    summer = Summer()
    receiver_service = ReceiverService(message_class=SumMessage, instance=summer)
    Thread(group=None, target=receiver_service.process).start()

    request = RequestHandle(SumMessage(1))
    request.send_async()
    assert request.wait() == 1

    try:
        request.send_and_wait_reply()
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed message reuse")

    request = RequestHandle(SumMessage(2))
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

    receiver_service.register_message(StartHandling(AddMessage))
    assert RequestHandle(AddMessage(1, 2)).send_and_wait_reply() == 3

    receiver_service.unregister_message(StopHandling(AddMessage))

    try:
        RequestHandle(AddMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    try:
        RequestHandle(SumMessage(1)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    receiver_service.register_message(StartHandling(SumMessage, summer))
    RequestHandle(SumMessage(1)).send_and_wait_reply()
    RequestHandle(SumMessage(2)).send_and_wait_reply()
    assert summer.sum == 3

    receiver_service.stop()


def test_service_control_messages(flush_redis):
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    assert RequestHandle(StartHandling(AddMessage)).send_and_wait_reply() == {AddMessage} | CORE_RECEIVER_MESSAGES
    try:
        RequestHandle(StartHandling(AddMessage)).send_and_wait_reply()
    except Exception:
        pass
    else:
        raise RuntimeError("Should have failed to re-add a message to a receiver.")

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    receivers_from_redis = dispatcher.get_receiver_services()
    assert receivers_from_redis[receiver_service.uid].registered_messages == {AddMessage} | CORE_RECEIVER_MESSAGES
    assert receivers_from_redis[receiver_service.uid].uid == receiver_service.uid

    assert receiver_service.get_registered_messages() == {AddMessage} | CORE_RECEIVER_MESSAGES
    assert RequestHandle(GetRegisteredMessages()).send_and_wait_reply() == {AddMessage} | CORE_RECEIVER_MESSAGES
    assert RequestHandle(AddMessage(1, 2)).send_and_wait_reply() == 3
    RequestHandle(StopHandling(AddMessage)).send_and_wait_reply()

    try:
        RequestHandle(AddMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    SumMessage.set_handler_instance_factory(lambda start: Summer(start))
    assert RequestHandle(StartHandling(SumMessage, {"start": 1})).send_and_wait_reply() == {
        SumMessage} | CORE_RECEIVER_MESSAGES
    assert RequestHandle(SumMessage(3)).send_and_wait_reply() == 4

    receiver_service.stop()


def test_queues(flush_redis):
    receiver_service = ReceiverService()

    RequestHandle(AddQueue(new_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' in receiver_service.listening_queues

    RequestHandle(StartHandling(AddMessage)).send_async()
    receiver_service.rdisq_process_one()

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    r = dispatcher.queue_task("ReceiverService_test_queue", AddMessage(1, 2))
    receiver_service.rdisq_process_one()
    assert r.wait() == 3

    RequestHandle(RemoveQueue(old_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' not in receiver_service.listening_queues


def test_multi(flush_redis):
    receiver_service_1 = ReceiverService()
    receiver_service_2 = ReceiverService()

    request = MultiRequestHandle(StartHandling(AddMessage)).send_async()
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    r = request.wait()
    assert r == [{AddMessage} | CORE_RECEIVER_MESSAGES, {AddMessage} | CORE_RECEIVER_MESSAGES]

    request = MultiRequestHandle(AddMessage(1, 3)).send_async()
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    r = request.wait()
    assert r == [4, 4]

    request = MultiRequestHandle(
        AddMessage(4, 4),
        lambda s: s.uid == receiver_service_1.uid).send_async()
    receiver_service_1.rdisq_process_one()
    r = request.wait()
    assert r == [8]

    receiver_service_3 = ReceiverService()
    request = MultiRequestHandle(
        AddMessage(4, 4),
        lambda s: s.uid in [receiver_service_1.uid, receiver_service_2.uid]).send_async()

    assert receiver_service_3.rdisq_process_one(1) is False
    receiver_service_1.rdisq_process_one()
    receiver_service_2.rdisq_process_one()
    assert request.wait() == [8, 8]
