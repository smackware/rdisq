from typing import *
from threading import Thread

from rdisq.request.rdisq_request import RdisqRequest, MultiRequest
from rdisq.request.dispatcher import RequestDispatcher, ReceiverServiceStatus
from rdisq.request.receiver import (
    ReceiverService, StartHandling, StopHandling, GetRegisteredMessages,
    CORE_RECEIVER_MESSAGES, AddQueue, RemoveQueue)
from rdisq.response import RdisqResponseTimeout
from tests._messages import SumMessage, sum_, AddMessage, SubtractMessage, Summer
from tests._other_module import MessageFromExternalModule

if TYPE_CHECKING:
    from tests.conftest import _RdisqMessageFixture


def test_message(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service = rdisq_message_fixture.spawn_receiver(message_class=SumMessage)

    call = RdisqRequest(SumMessage(1, 2)).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait(1) == 1 + 2

    message = SumMessage(3, 2)
    call = RdisqRequest(message).send_async()
    receiver_service.rdisq_process_one()
    assert call.wait(1) == sum_(message)

    rdisq_message_fixture.kill_all()


# =============================================


def test_class_message(rdisq_message_fixture: "_RdisqMessageFixture"):
    summer = Summer()
    receiver_service = rdisq_message_fixture.spawn_receiver(message_class=AddMessage, instance=summer)
    Thread(group=None, target=receiver_service.process).start()

    request = RdisqRequest(AddMessage(1))
    request.send_async()
    assert request.wait(1) == 1

    try:
        request.send_and_wait_reply()
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed message reuse")

    request = RdisqRequest(AddMessage(2))
    try:
        assert request.response
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed getting result before the evnet has run")

    assert request.send_and_wait_reply() == 3

    assert summer.sum == 3
    assert request.response.returned_value == 3
    rdisq_message_fixture.kill_all()


def test_dynamic_service(rdisq_message_fixture: "_RdisqMessageFixture"):
    summer = Summer()
    receiver_service = rdisq_message_fixture.spawn_receiver()
    Thread(group=None, target=receiver_service.process).start()

    receiver_service.register_message(StartHandling(SumMessage))
    assert RdisqRequest(SumMessage(1, 2)).send_and_wait_reply() == 3

    receiver_service.unregister_message(StopHandling(SumMessage))

    try:
        RdisqRequest(SumMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    try:
        RdisqRequest(AddMessage(1)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    receiver_service.register_message(StartHandling(AddMessage, summer))
    RdisqRequest(AddMessage(1)).send_and_wait_reply()
    RdisqRequest(AddMessage(2)).send_and_wait_reply()
    assert summer.sum == 3

    rdisq_message_fixture.kill_all()


def test_service_control_messages(rdisq_message_fixture):
    receiver_service = rdisq_message_fixture.spawn_receiver()
    Thread(group=None, target=receiver_service.process).start()

    assert RdisqRequest(StartHandling(SumMessage)).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    try:
        RdisqRequest(StartHandling(SumMessage)).send_and_wait_reply()
    except Exception:
        pass
    else:
        raise RuntimeError("Should have failed to re-sum_ a message to a receiver.")

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    receivers_from_redis = dispatcher.get_receiver_services()
    assert receivers_from_redis[receiver_service.uid].registered_messages == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert receivers_from_redis[receiver_service.uid].uid == receiver_service.uid

    assert receiver_service.get_registered_messages() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(GetRegisteredMessages()).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(SumMessage(1, 2)).send_and_wait_reply() == 3
    RdisqRequest(StopHandling(SumMessage)).send_and_wait_reply()

    try:
        RdisqRequest(SumMessage(1, 2)).send_and_wait_reply(1)
    except RdisqResponseTimeout:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    assert RdisqRequest(StartHandling(AddMessage, {"start": 1})).send_and_wait_reply() == {
        AddMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(AddMessage(3)).send_and_wait_reply() == 4

    rdisq_message_fixture.kill_all()


def test_queues(rdisq_message_fixture):
    receiver_service = rdisq_message_fixture.spawn_receiver()

    RdisqRequest(AddQueue(new_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' in receiver_service.listening_queues

    RdisqRequest(StartHandling(SumMessage)).send_async()
    receiver_service.rdisq_process_one()

    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    r = dispatcher.queue_task("ReceiverService_test_queue", SumMessage(1, 2))
    receiver_service.rdisq_process_one()
    assert r.wait(1) == 3

    RdisqRequest(RemoveQueue(old_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' not in receiver_service.listening_queues


def test_multi(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service_1 = rdisq_message_fixture.spawn_receiver()
    receiver_service_2 = rdisq_message_fixture.spawn_receiver()

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

    receiver_service_3 = rdisq_message_fixture.spawn_receiver()
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


def test_get_handler_class(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service_1 = rdisq_message_fixture.spawn_receiver()
    assert AddMessage.handler_factory._handler_class == Summer
    assert StartHandling.handler_factory._handler_class == type(receiver_service_1)
    assert MessageFromExternalModule.handler_factory._handler_class == _C
    assert SumMessage.handler_factory._handler_class is None


def test_handler_class_reuse(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service_1 = rdisq_message_fixture.spawn_receiver()
    RdisqRequest(StartHandling(AddMessage, {})).send_async()
    receiver_service_1.rdisq_process_one()
    RdisqRequest(StartHandling(SubtractMessage)).send_async()
    receiver_service_1.rdisq_process_one()

    r = RdisqRequest(AddMessage(4)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 4

    r = RdisqRequest(SubtractMessage(3)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 1


def test_custom_filter(rdisq_message_fixture: "_RdisqMessageFixture"):
    receivers: List[ReceiverService] = []
    for i in range(4):
        receivers.append(rdisq_message_fixture.spawn_receiver())

    request = MultiRequest(StartHandling(AddMessage, {})).send_async()
    for r in receivers:
        r.rdisq_process_one()
    request.wait(1)

    def filter_(receiver_status: ReceiverServiceStatus):
        return receiver_status.uid in [r.uid for r in receivers[:2]]

    request = MultiRequest(AddMessage(2), filter_).send_async()
    for r in receivers:
        r.rdisq_process_one(1)
    assert request.wait(1) == [2, 2]
    assert receivers[0]._handlers[AddMessage]._handler_instance.sum == 2
    assert receivers[1]._handlers[AddMessage]._handler_instance.sum == 2
    assert receivers[2]._handlers[AddMessage]._handler_instance.sum == 0
    assert receivers[3]._handlers[AddMessage]._handler_instance.sum == 0


