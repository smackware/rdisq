__author__ = 'smackware'

import time


from . import PROCESS_TIME_ATTR
from . import EXCEPTION_ATTR
from . import RESULT_ATTR
from serialization import decode


class RdisqResponseTimeout(Exception):
    task_id = None

    def __init__(self, task_id):
        self.task_id = task_id


class RdisqResponse(object):
    _task_id = None
    rdisq_consumer = None
    response_data = None
    process_time_seconds = None
    total_time_seconds = None
    called_at_unixtime = None
    timeout = None
    exception = None

    def __init__(self, task_id, rdisq_consumer):
        self._task_id = task_id
        self.rdisq_consumer = rdisq_consumer
        self.called_at_unixtime = time.time()

    def get_service_timeout(self):
        return self.rdisq_consumer.service_class.response_timeout

    def is_processed(self):
        redis_con = self.rdisq_consumer.get_redis()
        return redis_con.llen(self._task_id) > 0

    def is_exception(self):
        return self.exception is not None

    def process_response_data(self, decoded_response):
        self.response_data = decoded_response
        self.process_time_seconds = self.response_data[PROCESS_TIME_ATTR]
        self.exception = self.response_data[EXCEPTION_ATTR]
        return self.response_data[RESULT_ATTR]

    def wait(self):
        timeout = self.get_service_timeout()
        redis_con = self.rdisq_consumer.service_class.redis_dispatcher.get_redis()
        redis_response = redis_con.brpop(self._task_id, timeout=timeout)  # can be tuple of (queue_name, string) or None
        if redis_response is None:
            raise RdisqResponseTimeout(self._task_id)
        queue_name, response = redis_response
        self.total_time_seconds = time.time() - self.called_at_unixtime
        decoded_response = decode(response)
        redis_con.delete(self._task_id)
        self.process_response_data(decoded_response)
        if self.is_exception():
            raise self.exception
        return self.response_data[RESULT_ATTR]