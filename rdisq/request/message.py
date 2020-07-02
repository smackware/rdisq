from importlib import import_module

from typing import *


class MessageRequestData:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class RdisqMessage:
    _handler_name: ClassVar[str] = None
    _handler_instance_factory: ClassVar[Callable] = None
    _handler: ClassVar[Callable] = None
    _handler_class: ClassVar[Type] = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def set_handler_instance_factory(cls, new_factory: Callable):
        cls._handler_instance_factory = new_factory

    @classmethod
    def spawn_handler_instance(cls, *args, **kwargs):
        if cls._handler_instance_factory:
            return cls._handler_instance_factory(*args, **kwargs)
        elif cls.get_handler_class():
            return cls.get_handler_class()(*args, **kwargs)
        else:
            return None

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




    @classmethod
    def get_handler_class(cls) -> Optional[Type]:
        """
        :return: If this message's handler is an instance-method, then return there class where it's supposed to be.
        """
        if not cls._handler_class:
            path = cls._handler.__qualname__.split('.')
            module = import_module(cls._handler.__module__)
            try:
                handler_class = getattr(module, path[-2])
            except IndexError:
                handler_class = None

            if isinstance(handler_class, type):
                cls._handler_class = handler_class
            else:
                cls._handler_class = None

        return cls._handler_class
