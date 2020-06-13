from typing import *

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.service import RdisqService, remote_method


class RdisqEvent:
    _handler_name: str = None
    _handler: Callable = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self._sent = False
        self._result = None

    @classmethod
    def set_handler(cls, handler: Callable):
        """Decorator"""
        cls._handler = handler
        cls._handler_name = handler.__name__
        return handler

    def send(self, ):
        assert not self._sent, "This event has already been sent"
        self._sent = True
        r = EventWorker.get_consumer().send(
            self.get_handler_name(),
            **self.request_data
        ).wait()
        self._result = r
        return r

    @property
    def result(self):
        if self._sent:
            return self._result
        else:
            raise RuntimeError("not supposed to try to get results before the event was sent")

    @property
    def request_data(self) -> Dict[str, any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    @classmethod
    def get_handler_name(cls):
        return "%s.%s_handler" % (cls.__module__, cls.__name__)

    @classmethod
    def delegate_handler(cls, instance: object = None, *args, **kwargs):
        if not instance:
            return cls._handler(*args, **kwargs)
        else:
            return getattr(instance, cls._handler_name)(*args, **kwargs)

    @staticmethod
    def _handler(*args, **kwargs):
        pass


class EventWorker(RdisqService):
    service_name = "MyClass"
    response_timeout = 10  # seconds
    redis_dispatcher = PoolRedisDispatcher(host='127.0.0.1', port=6379, db=0)

    @remote_method
    def run_handler(self, *args, **kwargs):
        return self._rdisq_event_class.delegate_handler(
            instance=self._handler_instance, *args, **kwargs)

    def __init__(self, uid=None,
                 event_class: Type[RdisqEvent] = RdisqEvent,
                 instance: object = None
                 ):
        self._rdisq_event_class = event_class
        self._handler_instance = instance
        setattr(self, self._rdisq_event_class.get_handler_name(), self.run_handler)
        super().__init__(uid)
