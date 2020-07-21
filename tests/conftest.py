from typing import *
import pytest
from redis import Redis

from rdisq.configuration import get_rdisq_config
from rdisq.request.receiver import ReceiverService

if TYPE_CHECKING:
    from rdisq.request.message import RdisqMessage
    from rdisq.service import RdisqService


class _RdisqMessageFixture:
    def __init__(self):
        self.redis = Redis(host='127.0.0.1', port=6379, db=0)
        self.redis.flushdb()
        self.receivers: List["RdisqService"] = []

    def spawn_receiver(self, uid=None, message_class: Type["RdisqMessage"] = None,
                       instance: object = None) -> ReceiverService:
        _receiver = ReceiverService(uid, message_class, instance)
        self.receivers.append(_receiver)
        return _receiver

    def spawn_receivers(self, count: int) -> List["ReceiverService"]:
        new_receivers: List["RdisqService"] = []
        for i in range(count):
            new_receivers.append(self.spawn_receiver())
        return new_receivers

    def process_all_receivers(self, timeout=1):
        for r in self.receivers:
            r.rdisq_process_one(timeout)

    def kill_all(self):
        for r in self.receivers:
            r.stop()
            r.unregister_all()
            while r.is_active:
                pass
        while self.receivers:
            self.receivers.pop(0)


@pytest.fixture
def rdisq_message_fixture():

    r = _RdisqMessageFixture()
    yield r
    r.kill_all()
    del r
