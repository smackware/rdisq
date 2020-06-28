__author__ = 'smackware'

from typing import *
from .payload import RequestPayload

from .identification import generate_task_id
from .identification import get_request_key

from .response import RdisqResponse

if TYPE_CHECKING:
    from .service import RdisqService


class AbstractRdisqConsumer(object):
    service_class: "RdisqService"

    def __init__(self, service_class):
        self.__queue_to_callable = None
        self.service_class: RdisqService = service_class
        self.__setup_stub_methods_for_consumer()

    def __setup_stub_methods_for_consumer(self):
        self.__queue_to_callable = {}
        for attr in dir(self.service_class):
            call = getattr(self.service_class, attr)
            if not self.service_class.is_remote_method(call):
                continue
            stub_method_name = self.service_class.chop_prefix_from_exported_method_name(attr)
            setattr(self, stub_method_name, self.get_stub_method(stub_method_name))
            self.__queue_to_callable[stub_method_name] = call

    def send(self, method_name, *args, **kwargs):
        timeout = kwargs.pop("timeout", self.service_class.response_timeout)
        uid = kwargs.pop("rdisq_uid", None)
        method_queue_name = self.service_class.get_queue_name_for_method(method_name, uid)

        return self.service_class.redis_dispatcher.queue_task(
            method_queue_name, *args, timeout=timeout, **kwargs)

    def get_stub_method(self, method_name):
        raise NotImplementedError()


class RdisqAsyncConsumer(AbstractRdisqConsumer):
    def get_stub_method(self, method_name):
        def c(*args, **kwargs):
            return self.send(method_name, *args, **kwargs)
        return c


class RdisqWaitingConsumer(AbstractRdisqConsumer):
    def get_stub_method(self, method_name):
        def c(*args, **kwargs):
            return self.send(method_name, *args, **kwargs).wait()
        return c
