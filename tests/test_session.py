from typing import *

from rdisq.request.rdisq_request import RdisqRequest, MultiRequest
from rdisq.request.receiver import StartHandling
from rdisq.request.session import RdisqSession
from tests._messages import AddMessage

if TYPE_CHECKING:
    from tests.conftest import _RdisqMessageFixture


def test_session_base(rdisq_message_fixture: "_RdisqMessageFixture"):
    receivers = rdisq_message_fixture.spawn_receivers(5)

    MultiRequest(StartHandling(AddMessage, {})).send_async()
    rdisq_message_fixture.process_all_receivers()

    session = RdisqSession()
    session.send(AddMessage(2))
    receivers[-1].rdisq_process_one(1)
    assert session.wait(1) == 2
    assert session.current_request.returned_value == 2
    assert session._service_id == receivers[-1].uid

    session.send(AddMessage(2))
    rdisq_message_fixture.process_all_receivers()
    assert session.wait(1) == 4


def test_session_data(rdisq_message_fixture: "_RdisqMessageFixture"):
    receivers = rdisq_message_fixture.spawn_receiver()
    MultiRequest(StartHandling(AddMessage, {})).send_async()
    rdisq_message_fixture.process_all_receivers(1)
    session = RdisqSession()
    session.session_data = {"a": 3}
    session.send(AddMessage(2))
    session.session_data = {}
    rdisq_message_fixture.process_all_receivers(1)
    session.wait()
    assert session.session_data == {"a": 3}
