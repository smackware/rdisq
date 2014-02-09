import redis

class AbstractQueueConfig(object):
    def get_redis(self):
        raise NotImplementedError()

    def get_name(self):
        raise NotImplementedError()


class SimpleQueueConfig(AbstractQueueConfig):
    __queue_name = None

    def __init__(self, name):
        self.__queue_name = name

    def get_redis(self):
        return redis.Redis()

    def get_name(self):
        return self.__queue_name


class PoolQueueConfig(SimpleQueueConfig):
    REDIS_POOL = redis.ConnectionPool()

    def get_redis(self):
        return redis.Redis(connection_pool=self.REDIS_POOL)
