__author__ = 'smackware'

from typing import *
import time

from redis import Redis

if TYPE_CHECKING:
    from rdisq.redis_dispatcher import AbstractRedisDispatcher

if TYPE_CHECKING:
    from rdisq.payload import ResponsePayload
    from rdisq.consumer import AbstractRdisqConsumer


class RdisqResponseTimeout(Exception):
    task_id = None

    def __init__(self, task_id):
        self.task_id = task_id


class RdisqResponse(object):
    _task_id = None
    rdisq_consumer: "AbstractRdisqConsumer"
    response_payload: "ResponsePayload" = None
    total_time_seconds = None
    called_at_unixtime = None
    timeout = None
    exception = None
    default_timeout = 10

    @property
    def returned_value(self):
        return self.response_payload.returned_value

    def __init__(self, task_id, rdisq_consumer:"AbstractRdisqConsumer"=None, dispatcher: "AbstractRedisDispatcher" = None):
        if not rdisq_consumer and not dispatcher:
            raise RuntimeError("RdisqResponse initialized without consumer and without dispatcher.")

        self._task_id = task_id
        self.rdisq_consumer = rdisq_consumer
        if not dispatcher:
            self.dispatcher = rdisq_consumer.service_class.redis_dispatcher
        else:
            self.dispatcher = dispatcher
        self.called_at_unixtime = time.time()

    def get_service_timeout(self) -> int:
        if self.rdisq_consumer:
            return self.rdisq_consumer.service_class.response_timeout
        else:
            return self.default_timeout

    @property
    def redis_con(self)->Redis:
        return self.dispatcher.get_redis()

    def is_processed(self):
        if self.response_payload is not None:
            return True
        return self.redis_con.llen(self._task_id) > 0

    def is_exception(self):
        return self.response_payload.raised_exception is not None

    @property
    def process_time_seconds(self):
        return self.response_payload.processing_time_seconds

    @property
    def exception(self):
        return self.response_payload.raised_exception

    def wait(self, timeout=None):
        if not timeout:
            timeout = self.get_service_timeout()
        redis_response = self.redis_con.brpop(self._task_id,
                                         timeout=timeout)  # can be tuple of (queue_base_name, string) or None
        if redis_response is None:
            raise RdisqResponseTimeout(self._task_id)
        queue_name, response = redis_response
        return self.process_response(response)

    def process_response(self, response):
        self.total_time_seconds = time.time() - self.called_at_unixtime
        response_payload = self.dispatcher.serializer.loads(response)
        self.redis_con.delete(self._task_id)
        self.response_payload = response_payload
        if self.is_exception():
            raise self.exception
        return self.response_payload.returned_value
