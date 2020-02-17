__author__ = 'smackware'

import time


MISSING_DISPATCHER_ERROR_TEXT = \
    "Service class must have a 'redis_dispatcher' attribute pointing to a RedisDispatcher instance"
MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT = \
    "When instantiating a service in __main__, the service_name class attribute must be set"

from . import EXPORTED_METHOD_PREFIX

from .payload import RequestPayload
from .payload import ResponsePayload

from .identification import get_request_key
from .serialization import PickleSerializer

from .redis_dispatcher import LocalRedisDispatcher
from .consumer import RdisqAsyncConsumer
from .consumer import RdisqWaitingConsumer


# Decorator
def remote_method(callable_object):
    callable_object.is_remote = True
    return callable_object


class RdisqService(object):
    """
    Inheritance example:
    =============================================
    class CalculatorService(RdisqService):
        service_name = "CalculatorService"
        redis_dispatcher = LocalRedisDispatcher()

        @remote_method
        def add(self, a, b):
            return a + b;
    =============================================
    """
    logger = None
    log_returned_exceptions = True
    service_name = None
    response_timeout = 10
    redis_dispatcher = None
    serializer = PickleSerializer()
    __go = True
    __sync_consumer = None
    __async_consumer = None

    def __init__(self):
        if self.redis_dispatcher is None:
            raise NotImplementedError(MISSING_DISPATCHER_ERROR_TEXT)
        if self.__class__.__module__ == '__main__' and self.service_name is None:
            raise NotImplementedError(MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT)
        self.__queue_to_callable = None
        self.async = None
        self.__map_exposed_methods_to_queues()

    @property
    def rdisq_go(self):
        return self.__go

    def rdisq_process_one(self):
        self.__process_one()
        

    @classmethod
    def __set_service_name(cls, service_name):
        cls.service_name = service_name

    @classmethod
    def get_service_name(cls):
        if cls.service_name is None:
            cls.service_name = "%s.%s" % (cls.__module__, cls.__name__)
        return cls.service_name

    def __map_exposed_methods_to_queues(self):
        self.__queue_to_callable = self.get_queue_name_to_exposed_method_mapping()
        if not self.__queue_to_callable:
            raise AttributeError("Cannot instantiate a service with no exposed methods")

    @classmethod
    def get_method_if_exists(cls, name):
        method = cls.__dict__.get(name)
        if method is None or not hasattr(method, '__call__'):
            return None
        return method

    @staticmethod
    def is_remote_method(method):
        if method is None or not hasattr(method, '__call__'):
            return False
        if hasattr(method, 'is_remote'):
            return True
        # Legacy compatibility, remote methods used to be prefixed with 'q_'
        if method.__name__.startswith(EXPORTED_METHOD_PREFIX):
            return True
        return False

    def get_queue_name_to_exposed_method_mapping(self):
        mapping = {}
        for attr in dir(self):
            call = getattr(self, attr)
            if not self.is_remote_method(call):
                continue
            stub_method_name = self.chop_prefix_from_exported_method_name(attr)
            method_queue_name = self.get_queue_name_for_method(stub_method_name)
            mapping[method_queue_name] = call
        return mapping

    @classmethod
    def get_consumer(cls):
        if cls.__sync_consumer is None:
            cls.__sync_consumer = RdisqWaitingConsumer(cls)
        return cls.__sync_consumer

    @classmethod
    def get_async_consumer(cls):
        if cls.__async_consumer is None:
            cls.__async_consumer = RdisqAsyncConsumer(cls)
        return cls.__async_consumer

    def get_redis(self):
        return self.redis_dispatcher.get_redis()


    @classmethod
    def get_queue_name_for_method(cls, method_name):
        return cls.get_service_name() + "_" + method_name

    @classmethod
    def chop_prefix_from_exported_method_name(cls, method_name):
        # Legacy compatibility, remote methods used to be prefixed with 'q_'
        if method_name.startswith(EXPORTED_METHOD_PREFIX):
            return method_name[len(EXPORTED_METHOD_PREFIX):]
        return method_name

    def pre(self, method_queue_name):
        """Performs after something was found in the queue"""
        pass

    def post(self, method_queue_name):
        """Performs after a queue fetch and process"""
        pass

    def on_exception(self, exc):
        pass

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
        request_key = get_request_key(task_id.decode())
        call = self.__queue_to_callable[method_queue_name.decode()]
        data_string = redis_con.get(request_key)
        if data_string is None:
            return
        self.pre(method_queue_name)
        request_payload = self.serializer.loads(data_string)
        timeout = request_payload.timeout
        if request_payload.task_id != task_id.decode():
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
            if self.log_returned_exceptions and self.logger:
                self.logger.exception(ex)
            self.on_exception(ex)
        duration_seconds = time.time() - time_start
        response_payload = ResponsePayload(
            returned_value=result,
            processing_time_seconds=duration_seconds,
            raised_exception=raised_exception
        )
        serialized_response = self.serializer.dumps(response_payload)
        redis_con.lpush(task_id, serialized_response)
        redis_con.expire(task_id, timeout)
        self.post(method_queue_name)

    def process(self):
        self.on_start()
        while self.__go:
            self.__process_one()

    def stop(self):
        self.__go = False
