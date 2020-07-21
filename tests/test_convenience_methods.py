"""Test convenience methods that save the need for stuff such as instantiating factories"""
from threading import Thread
from typing import *

from rdisq.request.receiver import RegisterMessage
from tests._messages import AddMessage

if TYPE_CHECKING:
    from tests.conftest import _RdisqMessageFixture


def test_send_sync(rdisq_message_fixture: "_RdisqMessageFixture"):
    rdisq_message_fixture.spawn_receiver()
    r = RegisterMessage(AddMessage, {}).send_async()
    rdisq_message_fixture.process_all_receivers()
    assert AddMessage in r.wait()


def test_send_async(rdisq_message_fixture: "_RdisqMessageFixture"):
    receiver = rdisq_message_fixture.spawn_receiver()
    Thread(group=None, target=receiver.process).start()

    assert AddMessage in RegisterMessage(AddMessage, {}).send_and_wait()
    rdisq_message_fixture.kill_all()


