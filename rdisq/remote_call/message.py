from typing import *


class MessageRequestData:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class RdisqMessage:
    _handler_name: str = None
    _handler: Callable = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def set_handler(cls, handler: Callable):
        """Decorator"""
        cls._handler = handler
        cls._handler_name = handler.__name__
        return handler

    @classmethod
    def get_message_class_id(cls) -> str:
        return "%s.%s_handler" % (cls.__module__, cls.__name__)

    @classmethod
    def call_handler(cls, instance: object = None, *args, **kwargs):
        if not instance:
            return cls._handler(*args, **kwargs)
        else:
            return getattr(instance, cls._handler_name)(*args, **kwargs)

    @staticmethod
    def _handler(*args, **kwargs):
        raise RuntimeError("No handler set")
