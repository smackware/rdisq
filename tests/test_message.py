from threading import Thread

import pytest
from redis import Redis

from rdisq.remote_call.call import Call
from rdisq.remote_call.message import RdisqMessage, MessageRequestData
from rdisq.remote_call.receiver import (
    ReceiverService, StartHandling, StopHandling, GetRegisteredMessages)
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

    call = Call(AddMessage(1, 2))
    assert call.send() == add(1, 2)

    call = Call(AddMessage(3, 2))
    assert call.send() == add(3, 2)

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

    call = Call(SumMessage(1))
    assert call.send() == 1

    try:
        call.send()
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed message reuse")

    call = Call(SumMessage(2))
    try:
        assert call.response
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed getting result before the evnet has run")

    assert call.send() == 3

    assert summer.sum == 3
    assert call.response.returned_value == 3
    receiver_service.stop()


def test_dynamic_service(flush_redis):
    summer = Summer()
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    receiver_service.register_message(AddMessage)
    assert Call(AddMessage(1, 2)).send() == 3

    receiver_service.unregister_message(AddMessage)

    try:
        Call(AddMessage(1, 2)).send(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with worker")

    try:
        Call(SumMessage(1)).send(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with worker")

    receiver_service.register_message(SumMessage, summer)
    Call(SumMessage(1)).send()
    Call(SumMessage(2)).send()
    assert summer.sum == 4

    receiver_service.stop()


def test_service_control_messages(flush_redis):
    receiver_service = ReceiverService()
    Thread(group=None, target=receiver_service.process).start()

    assert Call(message=StartHandling(new_message_class=AddMessage)).send() == {
        GetRegisteredMessages, StartHandling, StopHandling, AddMessage}

    assert receiver_service.get_registered_messages() == {
        GetRegisteredMessages, StartHandling, StopHandling, AddMessage}
    assert Call(message=GetRegisteredMessages()).send() == {
        GetRegisteredMessages, StartHandling, StopHandling, AddMessage}
    assert Call(message=AddMessage(1, 2)).send() == 3
    Call(message=StopHandling(old_message_class=AddMessage)).send()

    try:
        Call(message=AddMessage(1, 2)).send(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with worker")

    SumMessage.set_handler_instance_factory(lambda start: Summer(start))
    assert Call(message=StartHandling(new_message_class=SumMessage, new_handler_kwargs={"start": 1})).send() == {
        GetRegisteredMessages, StartHandling, StopHandling, SumMessage}
    assert Call(SumMessage(3)).send() == 4

    receiver_service.stop()
