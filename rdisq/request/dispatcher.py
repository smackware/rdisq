from collections import defaultdict
from typing import *
import time

import uuid

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.consts import QueueName

if TYPE_CHECKING:
    from rdisq.request.receiver import ReceiverService


class ReceiverServiceStatus:
    """
    Describes a receiver worker.

    Meant to be generated and put in redis receiver_services upon heartbeat."""

    def __init__(self, worker: "ReceiverService"):
        self.registered_messages = worker.get_registered_messages()
        self.time = time.time()
        self.uid = worker.uid
        self.broadcast_queues: FrozenSet[QueueName] = worker.listening_queues
        self.tags: Dict = worker.tags


class RequestDispatcher(PoolRedisDispatcher):
    ACTIVE_SERVICES_REDIS_HASH = "receiver_services"

    def update_receiver_service_status(self, receiver: "ReceiverService")->ReceiverServiceStatus:
        status = ReceiverServiceStatus(receiver)
        self.get_redis().hset(self.ACTIVE_SERVICES_REDIS_HASH, key=status.uid,
                              value=receiver.serializer.dumps(status)
                              )
        return status

    def get_receiver_services(self) -> Dict[str, ReceiverServiceStatus]:
        raw_statuses: Dict[bytearray, bytearray] = self.get_redis().hgetall(self.ACTIVE_SERVICES_REDIS_HASH)
        statuses: Dict[str, ReceiverServiceStatus] = {}
        for k, v in raw_statuses.items():
            statuses[k.decode()] = self.serializer.loads(v)
        return statuses

    def filter_services(self, service_filter: Callable[["ReceiverServiceStatus"], bool]) -> Iterable[
        "ReceiverServiceStatus"]:
        """Filters the services-statuses in redis with the service_filter function, and returns those that pass

        :return: Statuses of services that match the filter.
        """
        services = self.get_receiver_services()
        return filter(service_filter, services.values())

    def find_queues_for_services(self, service_uids: Set[str]) -> FrozenSet[QueueName]:
        """
        Find all queues that are listened to by all these services.

        :param service_uids: Set IDs of queues to match.
        :return: Set of queue names.
        """
        services = self.get_receiver_services().values()
        queue_to_services: Dict[QueueName, set] = defaultdict(set)
        for service in services:
            for q in service.broadcast_queues:
                queue_to_services[q].add(service.uid)

        queue_to_services = {k: v for k, v in queue_to_services.items() if
                             v == service_uids}

        return frozenset(queue_to_services.keys())

    @staticmethod
    def generate_queue_name():
        """"""
        return f"rdisq_queue__{uuid.uuid4()}"
