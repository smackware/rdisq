from typing import *

from rdisq.request._handler import _HandlerFactory


class  RdisqMessage:
    """
    Data to be sent from client to server.

    On the server, each subclass needs to use @set_handler to choose a
    function that would receive the message.

    On the client side, use Request to send a message instance.
    """
    handler_factory: "_HandlerFactory" = None
    session_data: Dict = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def set_handler(cls, handler_function: Callable) -> Callable:
        """Decorator"""
        if cls.handler_factory:
            raise RuntimeError(f"Handler has already been set for {cls}."
                               f" Tried setting it to {handler_function}")
        else:
            cls.handler_factory = _HandlerFactory(handler_function)
            return handler_function

    @classmethod
    def get_message_class_id(cls) -> str:
        return "%s.%s_handler" % (cls.__module__, cls.__name__)


