__author__ = 'smackware'

import math
from typing import *
import time
import uuid
import logging

from .consts import QueueName
from rdisq.configuration import get_rdisq_config

if TYPE_CHECKING:
    from .redis_dispatcher import AbstractRedisDispatcher

MISSING_DISPATCHER_ERROR_TEXT = \
    "Service class must have a 'redis_dispatcher' attribute pointing to a RedisDispatcher instance"
MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT = \
    "When instantiating a service in __main__, the service_name class attribute must be set"

from . import EXPORTED_METHOD_PREFIX

from .payload import RequestPayload, SessionResult
from .payload import ResponsePayload

from .identification import get_request_key
from .serialization import PickleSerializer

from .redis_dispatcher import AbstractRedisDispatcher
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
    LOGGING_FORMAT = "%(levelname)s: %(name)s: %(message)s"

    logger: "logging.Logger" = None
    log_returned_exceptions = True
    service_name = None
    response_timeout = 10
    polling_timeout = 1
    redis_dispatcher: "AbstractRedisDispatcher" = None
    serializer: ClassVar[PickleSerializer] = PickleSerializer()
    __keep_working = True
    __sync_consumer = None
    __async_consumer = None
    __running_process_loops: int = 0

    _queue_to_callable: Dict[QueueName, Callable]
    _broadcast_queues: Set[QueueName]
    _direct_queues: Set[QueueName]

    def __init__(self, uid=None):
        if self.redis_dispatcher is None:
            raise NotImplementedError(MISSING_DISPATCHER_ERROR_TEXT)
        if self.__class__.__module__ == '__main__' and self.service_name is None:
            raise NotImplementedError(MISSING_SERVICE_NAME_IN_MAIN_ERROR_TEXT)
        self.__uid = uid or str(uuid.uuid4())
        if self.logger is None:
            self.logger = self.__setup_logger(self.__uid, logging.DEBUG)
        self.__is_suspended = False
        self.__map_exposed_methods_to_queues()

    def __setup_logger(self, name, level: int):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if len(logger.handlers) == 0:
            log_stream = logging.StreamHandler()
            formatter = logging.Formatter(self.LOGGING_FORMAT)
            log_stream.setFormatter(formatter)
            logger.addHandler(log_stream)
        return logger

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

    def get_redis(self) -> "Redis":
        return self.redis_dispatcher.get_redis()

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
        rdb = get_rdisq_config().request_dispatcher.get_redis()
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
        return self.__running_process_loops > 0

    @property
    def is_stopping(self):
        return not self.__keep_working

    @property
    def uid(self):
        """Returns the unique id of this service instance"""
        return self.__uid

    @property
    def listening_queues(self) -> FrozenSet[QueueName]:
        queues = self._direct_queues.copy()
        if not self.__is_suspended:
            queues |= self._broadcast_queues
        return frozenset(queues)

    @property
    def callables(self) -> FrozenSet[Callable]:
        """
        :return: A set of all the callables that might be triggered by messages to this Service's queues.
        """
        return frozenset(self._queue_to_callable.values())

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
        self.logger.info(f"Registering method to queue {direct_name}")
        self._queue_to_callable[direct_name] = method
        self._direct_queues.add(direct_name)

        broadcast_name = self.get_queue_name_for_method(queue_base_name)
        self._queue_to_callable[broadcast_name] = method
        self._broadcast_queues.add(broadcast_name)

    def unregister_all(self):
        # If we want to unregister messages while trying to stop - we must wait until full stop
        # otherwise we may be handling messages for removed queues
        if self.__keep_working is False:
            self.logger.warning("unregister_all requested while stopping, will have to wait until the service stops.")
            while self.is_active:
                pass
        self.logger.info(f"unregistering all handlers")
        while self._direct_queues:
            self._direct_queues.pop()
        while self._broadcast_queues:
            self._broadcast_queues.pop()

        for k in list(self._queue_to_callable.keys()):
            self._queue_to_callable.pop(k)

    def unregister_from_queue(self, queue_base_name):
        direct_name = self.get_queue_name_for_method(queue_base_name, self.__uid)
        self._direct_queues.remove(direct_name)
        self._queue_to_callable.pop(direct_name)

        broadcast_name = self.get_queue_name_for_method(queue_base_name)
        self._broadcast_queues.remove(broadcast_name)
        self._queue_to_callable.pop(broadcast_name)

    def wait_for_process_to_start(self, timeout=math.inf):
        start_time = time.time()
        while not self.__running_process_loops and time.time() - start_time < timeout:
            time.sleep(1)
        if not self.__running_process_loops:
            raise TimeoutError("No process started to run")

    def wait_for_process_to_stop(self, timeout=math.inf):
        start_time = time.time()
        while self.__running_process_loops and time.time() - start_time < timeout:
            time.sleep(1)
        if self.__running_process_loops:
            raise TimeoutError("Process did not stop in time")

    def process(self):
        self._on_start()
        redis_con = self.redis_dispatcher.get_redis()
        try:
            self._on_process_loop()
            self.__running_process_loops += 1
            while self.__keep_working:
                try:
                    self.__process_one(self.polling_timeout)
                    redis_con.hset(self.get_service_uid_list_key(), self.__uid, time.time())
                except Exception as e:
                    self.logger.exception(e)
                self._on_process_loop()
        finally:
            self.__running_process_loops -= 1
        self.logger.info("Stopped!")

    def stop(self):
        self.__keep_working = False

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
        self._queue_to_callable = {}
        self._broadcast_queues = set()
        self._direct_queues = set()

        for attr in dir(self):
            call = getattr(self, attr)
            if self.is_remote_method(call):
                self.register_method_to_queue(call, queue_base_name=attr)

        # if not self.__queue_to_callable:
        #     raise AttributeError("Cannot instantiate a service with no exposed methods")

    def __process_one(self, timeout=0):
        """Process a single queue_base_name event
        Will pend for an event (unless timeout is specified) then it will process it
        """
        try:
            redis_con = self.redis_dispatcher.get_redis()
            queues = list(self.listening_queues)
            redis_result = redis_con.brpop(queues, timeout=timeout)
        except ConnectionError:
            return

        if redis_result is None:  # Timeout
            return
        method_queue_name, task_id = redis_result
        request_key = get_request_key(task_id.decode())
        decoded_queue_name = method_queue_name.decode()
        if decoded_queue_name not in self._queue_to_callable:
            raise RuntimeError(f"{self.__uid}: Queue not found: {decoded_queue_name}")
        call = self._queue_to_callable[decoded_queue_name]
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
        if isinstance(result, SessionResult):
            session_data = result.session_data
            result = result.result
        else:
            session_data = None
        response_payload = ResponsePayload(
            returned_value=result,
            processing_time_seconds=duration_seconds,
            raised_exception=raised_exception,
            service_uid=self.uid,
            session_data=session_data
        )
        serialized_response = self.serializer.dumps(response_payload)
        redis_con.lpush(task_id, serialized_response)
        redis_con.expire(task_id, timeout)
        self._post(method_queue_name)
