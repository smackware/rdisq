__author__ = 'smackware'

from typing import *
import time
import uuid

from .consts import QueueName

if TYPE_CHECKING:
    from redis import Redis
    from .redis_dispatcher import AbstractRedisDispatcher

MISSING_DISPATCHER_ERROR_TEXT = \
    "Service class must have a 'redis_dispatcher' attribute pointing to a RedisDispatcher instance"
MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT = \
    "When instantiating a service in __main__, the service_name class attribute must be set"

from . import EXPORTED_METHOD_PREFIX

from .payload import RequestPayload
from .payload import ResponsePayload

from .identification import get_request_key
from .serialization import PickleSerializer

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
    polling_timeout = 1
    redis_dispatcher: ClassVar["AbstractRedisDispatcher"] = None
    serializer: ClassVar[PickleSerializer] = PickleSerializer()
    __go = True
    __sync_consumer = None
    __async_consumer = None

    __queue_to_callable: Dict[QueueName, Callable]
    __broadcast_queues: Set[QueueName]
    __direct_queues: Set[QueueName]

    def __init__(self, uid=None):
        if self.redis_dispatcher is None:
            raise NotImplementedError(MISSING_DISPATCHER_ERROR_TEXT)
        if self.__class__.__module__ == '__main__' and self.service_name is None:
            raise NotImplementedError(MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT)
        self.__is_suspended = False
        self.__uid = uid or str(uuid.uuid4())
        self.__map_exposed_methods_to_queues()

    @classmethod
    def get_service_name(cls):
        if cls.service_name is None:
            cls.service_name = "%s.%s" % (cls.__module__, cls.__name__)
        return cls.service_name

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

    @classmethod
    def get_consumer(cls) -> RdisqWaitingConsumer:
        if cls.__sync_consumer is None:
            cls.__sync_consumer = RdisqWaitingConsumer(cls)
        return cls.__sync_consumer

    @classmethod
    def get_async_consumer(cls):
        if cls.__async_consumer is None:
            cls.__async_consumer = RdisqAsyncConsumer(cls)
        return cls.__async_consumer

    @classmethod
    def get_redis(cls) -> "Redis":
        return cls.redis_dispatcher.get_redis()

    @classmethod
    def get_queue_name_for_method(cls, method_name, prefix=None) -> QueueName:
        if prefix is not None:
            return prefix + "_" + cls.get_service_name() + "_" + method_name
        return cls.get_service_name() + "_" + method_name

    @classmethod
    def chop_prefix_from_exported_method_name(cls, method_name):
        # Legacy compatibility, remote methods used to be prefixed with 'q_'
        if method_name.startswith(EXPORTED_METHOD_PREFIX):
            return method_name[len(EXPORTED_METHOD_PREFIX):]
        return method_name

    @classmethod
    def get_service_uid_list_key(cls):
        return "rdisq_uids:" + cls.get_service_name()

    @classmethod
    def list_uids(cls):
        uids = []
        rdb = cls.get_redis()
        key = cls.get_service_uid_list_key()
        for k, v in rdb.hgetall(key).items():
            if float(v) > time.time() - 10:
                uids.append(k.decode())
            else:
                rdb.hdel(key, k)
        return uids

    @classmethod
    def __set_service_name(cls, service_name):
        cls.service_name = service_name

    @property
    def is_active(self):
        return self.__go

    @property
    def uid(self):
        """Returns the unique id of this service instance"""
        return self.__uid

    @property
    def listening_queues(self) -> FrozenSet[QueueName]:
        queues = self.__direct_queues
        if not self.__is_suspended:
            queues = queues | self.__broadcast_queues
        return frozenset(queues)

    @property
    def callables(self) -> FrozenSet[Callable]:
        """
        :return: A set of all the callables that might be triggered by messages to this Service's queues.
        """
        return frozenset(self.__queue_to_callable.values())

    def rdisq_process_one(self, timeout=0):
        return self.__process_one(timeout=timeout)

    def suspend(self):
        self.__is_suspended = True

    def resume(self):
        self.__is_suspended = False

    def register_method_to_queue(self, method: Callable, queue_base_name: str = None):
        if not queue_base_name:
            queue_base_name = self.chop_prefix_from_exported_method_name(method.__name__)
        direct_name = self.get_queue_name_for_method(queue_base_name, self.__uid)
        self.__queue_to_callable[direct_name] = method
        self.__direct_queues.add(direct_name)

        broadcast_name = self.get_queue_name_for_method(queue_base_name)
        self.__queue_to_callable[broadcast_name] = method
        self.__broadcast_queues.add(broadcast_name)

    def unregister_from_queue(self, queue_base_name):
        direct_name = self.get_queue_name_for_method(queue_base_name, self.__uid)
        self.__direct_queues.remove(direct_name)
        self.__queue_to_callable.pop(direct_name)

        broadcast_name = self.get_queue_name_for_method(queue_base_name)
        self.__broadcast_queues.remove(broadcast_name)
        self.__queue_to_callable.pop(broadcast_name)

    def process(self):
        self._on_start()
        redis_con = self.get_redis()
        while self.__go:
            self.__process_one(self.polling_timeout)
            redis_con.hset(self.get_service_uid_list_key(), self.__uid, time.time())
            self._on_process_loop()

    def stop(self):
        self.__go = False

    def _pre(self, method_queue_name):
        """Performs after something was found in the queue_base_name"""
        pass

    def _post(self, method_queue_name):
        """Performs after a queue_base_name fetch and process"""
        pass

    def _on_exception(self, exc):
        pass

    def _on_start(self):
        pass

    def _on_process_loop(self):
        """Hook for doing stuff each cycle of the loop that waits for new messages in the queue"""
        pass

    def __map_exposed_methods_to_queues(self):
        self.__queue_to_callable = {}
        self.__broadcast_queues = set()
        self.__direct_queues = set()

        for attr in dir(self):
            call = getattr(self, attr)
            if self.is_remote_method(call):
                self.register_method_to_queue(call)

        # if not self.__queue_to_callable:
        #     raise AttributeError("Cannot instantiate a service with no exposed methods")

    def __process_one(self, timeout=0):
        """Process a single queue_base_name event
        Will pend for an event (unless timeout is specified) then it will process it
        """
        redis_con = self.get_redis()
        queues = list(self.listening_queues)
        redis_result = redis_con.brpop(queues, timeout=timeout)

        if redis_result is None:  # Timeout
            return False
        method_queue_name, task_id = redis_result
        request_key = get_request_key(task_id.decode())
        call = self.__queue_to_callable[method_queue_name.decode()]
        data_string = redis_con.get(request_key)
        if data_string is None:
            return
        self._pre(method_queue_name)
        request_payload: RequestPayload = self.serializer.loads(data_string)
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
            self._on_exception(ex)
        duration_seconds = time.time() - time_start
        response_payload = ResponsePayload(
            returned_value=result,
            processing_time_seconds=duration_seconds,
            raised_exception=raised_exception
        )
        serialized_response = self.serializer.dumps(response_payload)
        redis_con.lpush(task_id, serialized_response)
        redis_con.expire(task_id, timeout)
        self._post(method_queue_name)
