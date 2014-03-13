#!/usr/bin/env python
import os
import time
import uuid
import redis

"""
Terminology
================

request_key - a redis key containing the payload data of the request
queue - a redis queue with references to request_keys
response_key a redis key containing the payload data of the response of a certain request
"""

try:
    from cPickle import loads, dumps  #, UnpicklingError
except ImportError:  # noqa
    from pickle import loads, dumps  #, UnpicklingError

get_mac = lambda: uuid.getnode()

# Consts
TASK_ID_ATTR = "task_id"
ARGS_ATTR = "args"
KWARGS_ATTR = "kwargs"
RESULT_ATTR = "result"
PROCESS_TIME_ATTR = "process_time"
TIMEOUT_ATTR = "timeout"

EXPORTED_METHOD_PREFIX = "q_"

# Unique consumer ID
CONSUMER_ID = "%s-%s" % (get_mac(), os.getpid(), )


def generate_task_id():
    return "%s-%s" % (CONSUMER_ID, uuid.uuid4().hex, )


def encode(obj):
    return dumps(obj)


def decode(data):
    return loads(data)


class AbstractTaskException(Exception):
    def __init__(self, task_id):
        self.task_id = task_id


class ExpiredRequest(AbstractTaskException):
    pass


class ResultTimeout(AbstractTaskException):
    pass


class WorkerInitException(Exception):
    pass


class Result(object):
    _task_id = None
    consumer = None
    response = None
    process_time = None
    total_time = None
    _start = None
    timeout = None

    def __init__(self, task_id, consumer, timeout=None):
        self._task_id = task_id
        self.consumer = consumer
        self._start = time.time()
        if timeout is None:
            self.timeout = self.consumer.queue_config.get_timeout()

    # This method is deprecated
    def peek(self):
        return self.is_processed()

    def is_processed(self):
        redis_con = self.consumer.queue_config.get_redis()
        return redis_con.llen(self._task_id) > 0

    def wait(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        redis_con = self.consumer.queue_config.get_redis()
        redis_response = redis_con.brpop(self._task_id, timeout=timeout)  # can be tuple of (queue_name, string) or None
        if redis_response is None:
            raise ResultTimeout(self._task_id)
        queue_name, response = redis_response
        self.total_time = time.time() - self._start
        self.response = decode(response)
        self.process_time = self.response[PROCESS_TIME_ATTR]
        redis_con.delete(self._task_id)
        return self.response[RESULT_ATTR]


# Fugly right? I bet there's a better way to generate this dynamic object
class Async(object):
    def _reg_call(self, name, call):
        setattr(self, name, call)


class Rdisq(object):
    queue_config = None
    __go = True

    def __init__(self, queue_config):
        self.queue_config = queue_config
        self.__queue_to_callable = {}
        self.async = Async()
        for attr in dir(self):
            if attr.startswith(EXPORTED_METHOD_PREFIX):
                call = getattr(self, attr)
                method_name_sync = attr[len(EXPORTED_METHOD_PREFIX):]
                method_name_async = "async_" + method_name_sync
                queue_name = self.get_queue_name(method_name_sync)
                setattr(self, method_name_sync, self.__get_sync_method(self, queue_name))
                setattr(self, method_name_async, self.__get_async_method(self, queue_name))
                self.async._reg_call(method_name_sync, self.__get_async_method(self, queue_name))
                self.__queue_to_callable[queue_name] = call
        if not self.__queue_to_callable:
            raise WorkerInitException("Cannot instantiate a worker with no exposed methods")

    # Helper for restricting the scope
    @staticmethod
    def __get_async_method(parent, queue_name):
        def c(*args, **kwargs):
            return parent.send(queue_name, *args, **kwargs)
        return c

    # Helper for restricting the scope
    @staticmethod
    def __get_sync_method(parent, queue_name):
        def c(*args, **kwargs):
            last_exception = None
            for i in xrange(0, 3):
                try:
                    return parent.send(queue_name, *args, **kwargs).wait()
                except ResultTimeout as e:
                    last_exception = e
            raise last_exception
        return c

    @staticmethod
    def __get_request_key(task_id):
        return "request_%s" % (task_id, )

    def send(self, queue_name, *args, **kwargs):
        timeout = kwargs.pop("timeout", self.queue_config.get_timeout())
        redis_con = self.queue_config.get_redis()
        task_id = queue_name + generate_task_id()
        payload = {
            TASK_ID_ATTR: task_id,
            ARGS_ATTR: args,
            KWARGS_ATTR: kwargs,
            TIMEOUT_ATTR: timeout,
        }
        request_key = self.__get_request_key(task_id)
        redis_con.setex(request_key, encode(payload), timeout)
        redis_con.lpush(queue_name, task_id)
        return Result(task_id, self)

    def get_queue_name(self, method_name):
        return self.queue_config.get_name() + "_" + method_name

    def pre(self, queue_name):
        """Performs after something was found in the queue"""
        pass

    def post(self, queue_name):
        """Performs after a queue fetch and process"""
        pass

    def exception_handler(self, e):
        raise e

    def on_start(self):
        pass

    def __process_one(self, timeout=0):
        """Process a single queue event
        Will pend for an event (unless timeout is specified) then it will process it
        """
        redis_con = self.queue_config.get_redis()
        redis_result = redis_con.brpop(self.__queue_to_callable.keys(), timeout=timeout)
        if redis_result is None:  # Timeout
            return
        queue_name, task_id = redis_result
        request_key = self.__get_request_key(task_id)
        call = self.__queue_to_callable[queue_name]
        data_string = redis_con.get(request_key)
        if data_string is None:
            return
        self.pre(queue_name)
        task_data = decode(data_string)
        timeout = task_data.get(TIMEOUT_ATTR, 10)
        payload_task_id = task_data[TASK_ID_ATTR]
        if payload_task_id != task_id:
            # TODO: Though this situation is not expected to happen, I should still handle this raise better
            raise Exception("Severe error")
        args = task_data[ARGS_ATTR]
        kwargs = task_data[KWARGS_ATTR]
        start = time.time()
        result = call(*args, **kwargs)
        duration = time.time() - start
        response = {
            RESULT_ATTR: result,
            PROCESS_TIME_ATTR: duration,
        }
        response_string = encode(response)
        redis_con.lpush(task_id, response_string)
        redis_con.expire(task_id, timeout)
        self.post(queue_name)

    def process(self):
        self.on_start()
        while self.__go:
            try:
                self.__process_one()
            except Exception as e:
                self.exception_handler(e)


    def stop(self):
        self.__go = False
