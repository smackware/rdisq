from typing import *


class MessageRequestData:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class RdisqMessage:
    _handler_name: ClassVar[str] = None
    _handler_instance_factory: ClassVar[Callable] = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    # todo: should move this to a new Handler class
    @classmethod
    def set_handler_instance_factory(cls, new_factory: Callable):
        cls._handler_instance_factory = new_factory

    @classmethod
    def spawn_handler_instance(cls, *args, **kwargs):
        if not cls._handler_instance_factory:
            raise RuntimeError("Tried spawning a handler-new_handler_instance on a Message class without a factory")

        return cls._handler_instance_factory(*args, **kwargs)

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
    def call_handler(cls, message: "RdisqMessage", instance: object = None):
        if not instance:
            return cls._handler(message)
        else:
            return getattr(instance, cls._handler_name)(message)

    @staticmethod
    def _handler(*args, **kwargs):
        raise RuntimeError("No handler set")
