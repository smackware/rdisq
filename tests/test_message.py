from threading import Thread

import pytest
from redis import Redis

from rdisq.remote_call.request_handle import RequestHandle
from rdisq.remote_call.message import RdisqMessage, MessageRequestData
from rdisq.remote_call.message_dispatcher import MessageDispatcher
from rdisq.remote_call.receiver import (
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
def add(first, second):
    return first + second


def test_message(flush_redis):
    receiver_service = ReceiverService(message_class=AddMessage)
    Thread(group=None, target=receiver_service.process).start()

    call = RequestHandle(AddMessage(1, 2))
    assert call.send_and_wait_reply() == add(1, 2)

    call = RequestHandle(AddMessage(3, 2))
    assert call.send_and_wait_reply() == add(3, 2)

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
    def add(self, new):
        self.sum += new
        return self.sum


def test_class_message(flush_redis):
    summer = Summer()
    receiver_service = ReceiverService(message_class=SumMessage, instance=summer)
    Thread(group=None, target=receiver_service.process).start()

    request = RequestHandle(SumMessage(1))
    assert request.send_and_wait_reply() == 1

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

    receiver_service.register_message(AddMessage)
    assert RequestHandle(AddMessage(1, 2)).send_and_wait_reply() == 3

    receiver_service.unregister_message(AddMessage)

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

    receiver_service.register_message(SumMessage, summer)
    RequestHandle(SumMessage(1)).send_and_wait_reply()
    RequestHandle(SumMessage(2)).send_and_wait_reply()
    assert summer.sum == 3

    receiver_service.stop()


def test_service_control_messages(flush_redis):
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    assert RequestHandle(StartHandling(AddMessage)).send_and_wait_reply() == {AddMessage}.union(CORE_RECEIVER_MESSAGES)
    try:
        RequestHandle(StartHandling(AddMessage)).send_and_wait_reply()
    except Exception:
        pass
    else:
        raise RuntimeError("Should have failed to re-add a message to a receiver.")

    dispatcher = MessageDispatcher(host='127.0.0.1', port=6379, db=0)
    receivers_from_redis = dispatcher.get_receiver_services()
    assert receivers_from_redis[receiver_service.uid].registered_messages == {AddMessage}.union(CORE_RECEIVER_MESSAGES)
    assert receivers_from_redis[receiver_service.uid].uid == receiver_service.uid

    assert receiver_service.get_registered_messages() == {AddMessage}.union(CORE_RECEIVER_MESSAGES)
    assert RequestHandle(GetRegisteredMessages()).send_and_wait_reply() == {AddMessage}.union(CORE_RECEIVER_MESSAGES)
    assert RequestHandle(AddMessage(1, 2)).send_and_wait_reply() == 3
    RequestHandle(StopHandling(AddMessage)).send_and_wait_reply()

    try:
        RequestHandle(AddMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    SumMessage.set_handler_instance_factory(lambda start: Summer(start))
    assert RequestHandle(StartHandling(SumMessage, {"start": 1})).send_and_wait_reply() == {SumMessage}.union(CORE_RECEIVER_MESSAGES)
    assert RequestHandle(SumMessage(3)).send_and_wait_reply() == 4

    receiver_service.stop()


def test_queues(flush_redis):
    receiver_service = ReceiverService()


    RequestHandle(AddQueue(new_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' in receiver_service.broadcast_queues

    RequestHandle(StartHandling(AddMessage)).send_async()
    receiver_service.rdisq_process_one()

    dispatcher = MessageDispatcher(host='127.0.0.1', port=6379, db=0)
    r = dispatcher.queue_task("ReceiverService_test_queue", AddMessage(1, 2))
    receiver_service.rdisq_process_one()
    assert r.wait() == 3

    RequestHandle(RemoveQueue(old_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' not in receiver_service.broadcast_queues


