import time
from typing import *
from threading import Thread

import pytest
from redis import Redis

from rdisq.configuration import get_rdisq_config
from rdisq.request.handler import _HandlerFactory
from rdisq.request.rdisq_request import RdisqRequest, MultiRequest
from rdisq.request.dispatcher import RequestDispatcher, ReceiverServiceStatus
from rdisq.request.receiver import (
    ReceiverService, RegisterMessage, UnregisterMessage, GetRegisteredMessages, RegisterAll,
    CORE_RECEIVER_MESSAGES, AddQueue, RemoveQueue, SetReceiverTags, ShutDownReceiver)
from rdisq.response import RdisqResponseTimeout
from tests._messages import SumMessage, sum_, AddMessage, SubtractMessage, Summer
from tests._other_module import MessageFromExternalModule

if TYPE_CHECKING:
    from tests.conftest import _RdisqMessageFixture


def _basic_summer_test(rdisq_message_fixture: "_RdisqMessageFixture"):
    request = AddMessage(1).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert request.wait() == 3

    request = SubtractMessage(1).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert request.wait() == 2


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
    receiver_service.wait_for_process_to_start(3)

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
    receiver_service.wait_for_process_to_start(3)

    receiver_service.register_message(RegisterMessage(SumMessage))
    assert RdisqRequest(SumMessage(1, 2)).send_and_wait_reply() == 3

    receiver_service.unregister_message(UnregisterMessage(SumMessage))

    try:
        RdisqRequest(SumMessage(1, 2)).send_and_wait_reply(1)
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    try:
        RdisqRequest(AddMessage(1)).send_and_wait_reply(1)
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    receiver_service.register_message(RegisterMessage(AddMessage, summer))
    RdisqRequest(AddMessage(1)).send_and_wait_reply()
    RdisqRequest(AddMessage(2)).send_and_wait_reply()
    assert summer.sum == 3

    rdisq_message_fixture.kill_all()


def test_shutdown_message(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service = rdisq_message_fixture.spawn_receiver()
    Thread(group=None, target=receiver_service.process).start()
    receiver_service.wait_for_process_to_start()
    assert {AddMessage} < RegisterMessage(AddMessage, {"start": 2}).send_and_wait()
    assert AddMessage(3).send_and_wait() == 5
    ShutDownReceiver().send_and_wait()

    with pytest.raises(RuntimeError, match=r"Tried sending a request, but not suitable receiver services were found."):
        AddMessage(3).send_and_wait(timeout=1)

    receiver_service.wait_for_process_to_stop()
    assert not receiver_service.is_active


def test_service_control_messages(rdisq_message_fixture):
    receiver_service = rdisq_message_fixture.spawn_receiver()
    Thread(group=None, target=receiver_service.process).start()
    receiver_service.wait_for_process_to_start(5)
    assert RdisqRequest(RegisterMessage(SumMessage)).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    try:
        RdisqRequest(RegisterMessage(SumMessage)).send_and_wait_reply()
    except Exception:
        pass
    else:
        raise RuntimeError("Should have failed to re-sum_ a message to a receiver.")

    dispatcher = get_rdisq_config().request_dispatcher
    receivers_from_redis = dispatcher.get_receiver_services()
    assert receivers_from_redis[receiver_service.uid].registered_messages == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert receivers_from_redis[receiver_service.uid].uid == receiver_service.uid

    assert receiver_service.get_registered_messages() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(GetRegisteredMessages()).send_and_wait_reply() == {SumMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(SumMessage(1, 2)).send_and_wait_reply() == 3
    RdisqRequest(UnregisterMessage(SumMessage)).send_and_wait_reply()

    try:
        RdisqRequest(SumMessage(1, 2)).send_and_wait_reply(1)
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Should have failed communicating with receiver")

    assert RdisqRequest(RegisterMessage(AddMessage, {"start": 1})).send_and_wait_reply() == {
        AddMessage} | CORE_RECEIVER_MESSAGES
    assert RdisqRequest(AddMessage(3)).send_and_wait_reply() == 4

    rdisq_message_fixture.kill_all()


def test_queues(rdisq_message_fixture):
    receiver_service = rdisq_message_fixture.spawn_receiver()

    RdisqRequest(AddQueue(new_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    assert 'ReceiverService_test_queue' in receiver_service.listening_queues

    RdisqRequest(RegisterMessage(SumMessage)).send_async()
    receiver_service.rdisq_process_one()

    dispatcher = get_rdisq_config().request_dispatcher
    r = dispatcher.queue_task("ReceiverService_test_queue", SumMessage(1, 2))
    receiver_service.rdisq_process_one()
    assert r.wait(1) == 3

    r = RdisqRequest(RemoveQueue(old_queue_name="test_queue")).send_async()
    receiver_service.rdisq_process_one()
    result = r.wait()
    assert 'ReceiverService_test_queue' not in result
    assert 'ReceiverService_test_queue' not in receiver_service.listening_queues


def test_multi(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service_1 = rdisq_message_fixture.spawn_receiver()
    receiver_service_2 = rdisq_message_fixture.spawn_receiver()
    request = MultiRequest(RegisterMessage(SumMessage)).send_async()
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

    def _get_handler_class(message_class: Type["RdisqMessage"]):
        return get_rdisq_config().handler_factory._get_handler_class_for_function(
            _HandlerFactory._messages_registered_handlers[message_class])

    assert _get_handler_class(AddMessage) == Summer
    assert _get_handler_class(RegisterMessage) == type(receiver_service_1)
    assert _get_handler_class(MessageFromExternalModule) == _C
    assert _get_handler_class(SumMessage) is None


def test_handler_class_reuse(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver_service_1 = rdisq_message_fixture.spawn_receiver()
    RdisqRequest(RegisterMessage(AddMessage, {})).send_async()
    receiver_service_1.rdisq_process_one()
    RdisqRequest(RegisterMessage(SubtractMessage)).send_async()
    receiver_service_1.rdisq_process_one()

    r = RdisqRequest(AddMessage(4)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 4

    r = RdisqRequest(SubtractMessage(3)).send_async()
    receiver_service_1.rdisq_process_one()
    assert r.wait(1) == 1


def test_custom_filter_with_tags(rdisq_message_fixture: "_RdisqMessageFixture"):
    receivers: List[ReceiverService] = rdisq_message_fixture.spawn_receivers(4)

    request = MultiRequest(RegisterAll({}, Summer)).send_async()
    rdisq_message_fixture.process_all_receivers()
    request.wait(1)

    def filter_(receiver_status: ReceiverServiceStatus):
        return receiver_status.uid in [r.uid for r in receivers[:2]]

    example_tags = {'foo': 'bar'}
    request = MultiRequest(SetReceiverTags(example_tags), filter_).send_async()
    rdisq_message_fixture.process_all_receivers()

    assert request.wait(1) == [example_tags, example_tags]

    def filter_2_(receiver_status: ReceiverServiceStatus):
        return receiver_status.tags.get('foo') == 'bar'

    request = MultiRequest(AddMessage(3), filter_2_).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert request.wait() == [3, 3]

    assert receivers[0]._handlers[AddMessage]._handler_instance.sum == 3
    assert receivers[1]._handlers[AddMessage]._handler_instance.sum == 3
    assert receivers[2]._handlers[AddMessage]._handler_instance.sum == 0
    assert receivers[3]._handlers[AddMessage]._handler_instance.sum == 0


def test_register_entire_class_with_kwargs(rdisq_message_fixture: "_RdisqMessageFixture"):
    rdisq_message_fixture.spawn_receiver()
    request = RegisterAll({"start": 2}, Summer).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert {AddMessage, SubtractMessage} < request.wait()

    _basic_summer_test(rdisq_message_fixture)


def test_register_entire_class_with_instance(rdisq_message_fixture: "_RdisqMessageFixture"):
    rdisq_message_fixture.spawn_receiver()
    s = Summer(2)
    request = RegisterAll(s).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert {AddMessage, SubtractMessage} < request.wait()

    _basic_summer_test(rdisq_message_fixture)


def test_another_server(rdisq_message_fixture: "_RdisqMessageFixture"):
    dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=2)
    rdisq_message_fixture.redis = Redis(host='127.0.0.1', port=6379, db=2)
    rdisq_message_fixture.redis.flushdb()

    rdisq_message_fixture.receivers.append(ReceiverService(dispatcher=dispatcher))

    request = RegisterAll({"start": 2}, Summer).send_async(request_dispatcher=dispatcher)
    rdisq_message_fixture.process_all_receivers()
    assert {AddMessage, SubtractMessage} < request.wait()
