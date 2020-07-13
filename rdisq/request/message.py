from typing import *

from rdisq.consts import ServiceUid
from rdisq.request._handler import _HandlerFactory

if TYPE_CHECKING:
    from rdisq.request.dispatcher import ReceiverServiceStatus
    from rdisq.request.rdisq_request import RdisqRequest


class RdisqMessage:
    """
    Data to be sent from client to server.

    On the server, each subclass needs to use @set_handler to choose a
    function that would receive the message.

    On the client side, use Request to send_async a message instance.
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

    # ===================================================================
    # Convenience Methods
    # ===================================================================

    def send_async(self, service_filter: Callable[["ReceiverServiceStatus"], bool] = None,
                   targets: Set[ServiceUid] = None) -> "RdisqRequest":
        """Generate a request for this message, send it, and return the request handle
        :return: The request that was send with this message
        """
        # if we import this at module level, it would cause a circular import
        from rdisq.request.rdisq_request import RdisqRequest
        return RdisqRequest(self, service_filter, targets).send_async()

    def send_and_wait(self, service_filter: Callable[["ReceiverServiceStatus"], bool] = None,
                      targets: Set[ServiceUid] = None) -> Any:
        """Generate a request for this message, send it, wait for it to finish, and return the result
        :return: The result of the handler that handled the message
        """
        return self.send_async(service_filter, targets).wait()
