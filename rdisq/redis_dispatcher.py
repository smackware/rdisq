__author__ = 'smackware'

from redis import Redis
from redis import ConnectionPool


class AbstractRedisDispatcher(object):

    def __init__(self, *args, **kwargs):
        pass

    def get_redis(self, *args, **kwargs):
        """
        Produce an instance of an active redis connection
        """
        raise NotImplementedError("Must implement get_redis(self) method of Rdisq subclass")

    def close(self):
        raise NotImplementedError("Must implement close(self) of dispatcher")


class SimpleRedisDispatcher(AbstractRedisDispatcher):
    redis_args = None
    redis_kwargs = None

    def __init__(self, *redis_args, **redis_kwargs):
        self.redis_args = redis_args
        self.redis_kwargs = redis_kwargs
        AbstractRedisDispatcher.__init__(self)
        self.redis = Redis(*self.redis_args, **self.redis_kwargs)

    def get_redis(self):
        return self.redis

    def close(self):
        self.redis.close()


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

    def close(self):
        self.redis_pool.disconnect()
