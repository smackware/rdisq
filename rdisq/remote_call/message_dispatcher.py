from typing import *
import time

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.serialization import PickleSerializer

if TYPE_CHECKING:
    from rdisq.remote_call.receiver import ReceiverService


class ReceiverServiceStatus:
    """For serializing and putting in redis"""

    def __init__(self, worker: "ReceiverService"):
        self.registered_messages = worker.get_registered_messages()
        self.time = time.time()
        self.uid = worker.uid


class MessageDispatcher(PoolRedisDispatcher):
    ACTIVE_SERVICES_REDIS_HASH = "receiver_services"
    serializer: ClassVar[PickleSerializer] = PickleSerializer()


    def update_receiver_service_status(self, receiver: "ReceiverService"):
        status = ReceiverServiceStatus(receiver)
        self.get_redis().hset(self.ACTIVE_SERVICES_REDIS_HASH, key=status.uid,
                              value=receiver.serializer.dumps(status)
                              )

    def get_receiver_services(self) -> Dict[str, ReceiverServiceStatus]:
        raw_statuses: Dict[bytearray, bytearray] = self.get_redis().hgetall(self.ACTIVE_SERVICES_REDIS_HASH)
        statuses: Dict[str, ReceiverServiceStatus] = {}
        for k, v in raw_statuses.items():
            statuses[k.decode()] = self.serializer.loads(v)
        return statuses
