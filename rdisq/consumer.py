__author__ = 'smackware'


from . import EXPORTED_METHOD_PREFIX
from payload import ResponsePayload
from payload import RequestPayload

from serialization import encode

from identification import generate_task_id
from identification import get_request_key

from response import RdisqResponse


class RdisqAsyncConsumer(object):
    def register_call_(self, name, call):
        setattr(self, name, call)


class AbstractRdisqConsumer(object):
    service_class = None

    def __init__(self, service_class):
        self.__queue_to_callable = None
        self.service_class = service_class
        self.__setup_stub_methods_for_consumer()

    def __setup_stub_methods_for_consumer(self):
        self.__queue_to_callable = {}
        for attr in dir(self.service_class):
            if attr.startswith(EXPORTED_METHOD_PREFIX):
                call = getattr(self.service_class, attr)
                stub_method_name = self.service_class.chop_prefix_from_exported_method_name(attr)
                method_queue_name = self.service_class.get_queue_name_for_method(stub_method_name)
                setattr(self, stub_method_name, self.get_stub_method(method_queue_name))
                self.__queue_to_callable[method_queue_name] = call
        if not self.__queue_to_callable:
            raise AttributeError("Cannot instantiate a consumer with no exposed methods")

    def send(self, method_queue_name, *args, **kwargs):
        timeout = kwargs.pop("timeout", self.service_class.response_timeout)
        redis_con = self.service_class.redis_dispatcher.get_redis()
        task_id = method_queue_name + generate_task_id()
        request_payload = RequestPayload(
            task_id=task_id,
            args=args,
            kwargs=kwargs,
            timeout=timeout
        )
        request_key = get_request_key(task_id)
        redis_con.setex(request_key, encode(request_payload), timeout)
        redis_con.lpush(method_queue_name, task_id)
        return RdisqResponse(task_id, self)

    def get_stub_method(self, method_queue_name):
        raise NotImplementedError()


class RdisqAsyncConsumer(AbstractRdisqConsumer):
    def get_stub_method(self, method_queue_name):
        def c(*args, **kwargs):
            return self.send(method_queue_name, *args, **kwargs)
        return c


class RdisqWaitingConsumer(AbstractRdisqConsumer):
    def get_stub_method(self, method_queue_name):
        def c(*args, **kwargs):
            return self.send(method_queue_name, *args, **kwargs).wait()
        return c