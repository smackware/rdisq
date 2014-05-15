__author__ = 'smackware'

import time


MISSING_DISPATCHER_ERROR_TEXT = \
    "Service class must have a 'redis_dispatcher' attribute pointing to a RedisDispatcher instance"
MISSING_SERVICE_NAME_ERROR_TEXT = \
    "Missing service_name class attribute"

from . import EXPORTED_METHOD_PREFIX

from payload import RequestPayload
from payload import ResponsePayload

from identification import get_request_key
from serialization import encode, decode

from redis_dispatcher import LocalRedisDispatcher
from consumer import RdisqAsyncConsumer
from consumer import RdisqWaitingConsumer


class RdisqService(object):
    service_name = None
    response_timeout = 10
    redis_dispatcher = None
    __go = True
    __waiting_consumer = None
    __async_consumer = None

    def __init__(self):
        if self.service_name is None:
            raise NotImplementedError(MISSING_SERVICE_NAME_ERROR_TEXT)
        if self.redis_dispatcher is None:
            raise NotImplementedError(MISSING_DISPATCHER_ERROR_TEXT)
        self.__queue_to_callable = None
        self.async = None
        self.__map_exposed_methods_to_queues()

    def __map_exposed_methods_to_queues(self):
        self.__queue_to_callable = self.get_queue_name_to_exposed_method_mapping()
        if not self.__queue_to_callable:
            raise AttributeError("Cannot instantiate a service with no exposed methods")

    def get_queue_name_to_exposed_method_mapping(self):
        mapping = {}
        for attr in dir(self):
            if not attr.startswith(EXPORTED_METHOD_PREFIX):
                continue
            call = getattr(self, attr)
            stub_method_name = self.chop_prefix_from_exported_method_name(attr)
            method_queue_name = self.get_queue_name_for_method(stub_method_name)
            mapping[method_queue_name] = call
        return mapping

    @classmethod
    def get_consumer(cls):
        if cls.__waiting_consumer is None:
            cls.__waiting_consumer = RdisqWaitingConsumer(cls)
        return cls.__waiting_consumer

    @classmethod
    def get_async_consumer(cls):
        if cls.__async_consumer is None:
            cls.__async_consumer = RdisqAsyncConsumer(cls)
        return cls.__async_consumer

    def get_redis(self):
        return self.redis_dispatcher.get_redis()


    @classmethod
    def get_queue_name_for_method(cls, method_name):
        return cls.service_name + "_" + method_name

    @classmethod
    def chop_prefix_from_exported_method_name(cls, method_name):
        return method_name[len(EXPORTED_METHOD_PREFIX):]

    def init(self, *args, **kwargs):
        """Run on instatiation, use this instead of __init__"""
        pass

    def pre(self, method_queue_name):
        """Performs after something was found in the queue"""
        pass

    def post(self, method_queue_name):
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
        redis_con = self.get_redis()
        redis_result = redis_con.brpop(self.__queue_to_callable.keys(), timeout=timeout)
        if redis_result is None:  # Timeout
            return
        method_queue_name, task_id = redis_result
        request_key = get_request_key(task_id)
        call = self.__queue_to_callable[method_queue_name]
        data_string = redis_con.get(request_key)
        if data_string is None:
            return
        self.pre(method_queue_name)
        request_payload = decode(data_string)
        timeout = request_payload.timeout
        if request_payload.task_id != task_id:
            raise ValueError("Memorized task id is mismatching to received-payload task_id")
        args = request_payload.args
        kwargs = request_payload.kwargs
        time_start = time.time()
        try:
            result = call(*args, **kwargs)
            raised_exception = None
        except Exception as ex:
            result = None
            raised_exception = ex
        duration_seconds = time.time() - time_start
        response_payload = ResponsePayload(
            returned_value=result,
            processing_time_seconds=duration_seconds,
            raised_exception=raised_exception
        )
        response_string = encode(response_payload)
        redis_con.lpush(task_id, response_string)
        redis_con.expire(task_id, timeout)
        self.post(method_queue_name)

    def process(self):
        self.on_start()
        while self.__go:
            self.__process_one()

    def stop(self):
        self.__go = False
