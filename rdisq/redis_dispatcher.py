__author__ = 'smackware'

from typing import ClassVar

from redis import Redis
from redis import ConnectionPool

from rdisq.identification import generate_task_id, get_request_key
from rdisq.payload import RequestPayload
from rdisq.response import RdisqResponse

from rdisq.serialization import PickleSerializer


class AbstractRedisDispatcher(object):
    default_call_timeout = 10
    serializer: ClassVar[PickleSerializer] = PickleSerializer()

    def __init__(self, *args, **kwargs):
        pass

    def get_redis(self, *args, **kwargs) -> Redis:
        """
        Produce an instance of an active redis connection
        """
        raise NotImplementedError("Must implement get_redis(self) method of Rdisq subclass")

    def queue_task(self, queue_name: str, *task_args, timeout=None, **task_kwargs):
        if not timeout:
            timeout = self.default_call_timeout
        task_id = queue_name + generate_task_id()
        request_payload = RequestPayload(
            task_id=task_id,
            args=task_args,
            kwargs=task_kwargs,
            timeout=timeout
        )

        redis_con = self.get_redis()
        # todo -- ask lital why he isn't passing the serialized_request inside the lpush? wouldn't that save a network call?
        redis_con.setex(get_request_key(task_id), timeout,
                        self.serializer.dumps(request_payload))
        redis_con.lpush(queue_name, task_id)

        return RdisqResponse(task_id, dispatcher=self)


class SimpleRedisDispatcher(AbstractRedisDispatcher):
    redis_args = None
    redis_kwargs = None

    def __init__(self, *redis_args, **redis_kwargs):
        self.redis_args = redis_args
        self.redis_kwargs = redis_kwargs
        AbstractRedisDispatcher.__init__(self)

    def get_redis(self):
        return Redis(*self.redis_args, **self.redis_kwargs)


class LocalRedisDispatcher(SimpleRedisDispatcher):
    def __init__(self):
        SimpleRedisDispatcher.__init__(self)


class PoolRedisDispatcher(AbstractRedisDispatcher):
    redis_pool = None

    def __init__(self, *pool_args, **pool_kwargs):
        self.redis_pool = ConnectionPool(*pool_args, **pool_kwargs)
        AbstractRedisDispatcher.__init__(self)

    def get_redis(self):
        return Redis(connection_pool=self.redis_pool)
