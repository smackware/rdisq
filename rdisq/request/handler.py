from typing import *
from importlib import import_module

if TYPE_CHECKING:
    from rdisq.request.message import RdisqMessage


class _Handler:
    def __init__(self, handler_function: Callable[["RdisqMessage"], Any],
                 handler_class: Type = None,
                 instance: Union[Dict, object] = None,
                 siblings: Iterable["_Handler"] = frozenset()
                 ):
        """
        :param handler_function: A function that will handle messages
        :param handler_class: If the handler function is a bound-method, this should be the class that contains it.
        :param instance: For bound-methods, this is either an instance that holds it, or kwargs for creating one.
        :param siblings: Other handlers that live in the same receiver. Will try to reuse their handler-instances if
            the handler_function is a bound method and instance is not provided.
        """
        self._handler_function = handler_function
        self._handler_class = handler_class
        self._handler_name = self._handler_function.__name__
        error = None

        new_handler_instance = None
        if isinstance(instance, dict):
            if self._handler_class:
                new_handler_instance = self._handler_class(**instance)
            else:
                error = "Provided kwargs for constructor, but no class to _handler_construct"
        elif self._handler_class and isinstance(instance, self._handler_class):
            new_handler_instance = instance
        elif instance is None:
            if self._handler_class is None:
                new_handler_instance = None
            else:
                for sibling in siblings:
                    if isinstance(sibling._handler_instance, self._handler_class):
                        new_handler_instance = sibling._handler_instance
                        break
                if not new_handler_instance:
                    error = f"Did not provide handler instance and no suitable instance was found among siblings"
        else:
            error = f"invalid handler instance {instance}"

        if error:
            raise RuntimeError(error)
        else:
            self._handler_instance = new_handler_instance

    def handle(self, message: "RdisqMessage") -> Any:
        if not self._handler_instance:
            return self._handler_function(message)
        else:
            return getattr(self._handler_instance, self._handler_name)(message)


T = TypeVar('T', bound=type)


class _HandlerFactory(Generic[T]):
    _handler_function: Callable = None
    _messages_registered_handlers: Dict[Type["RdisqMessage"], Callable] = {}

    def set_handler_function(self, handler_function: Callable, message_class: Type["RdisqMessage"]):
        if message_class in self._messages_registered_handlers:
            raise RuntimeError(f"Handler has already been set for {message_class}."
                               f" Tried setting it to {handler_function}")
        else:
            self._messages_registered_handlers[message_class] = handler_function
            return handler_function

    def create_handler(self, message_class: Type["RdisqMessage"],
                       instance_param: Union[Dict, object] = None,
                       sibling_handlers: Iterable[_Handler] = frozenset(),
                       ) -> _Handler:
        registered_function: Callable = self._messages_registered_handlers[message_class]
        return _Handler(registered_function, handler_class=self._get_handler_class_for_function(registered_function),
                        instance=instance_param, siblings=sibling_handlers)

    def create_handlers_for_object(self,
            new_handler_kwarg: Union[Dict, object],
            handler_class: type = None) -> Dict[Type["RdisqMessage"], "_Handler"]:
        """Create handler instances for any ragistered handler-methods in the object"""
        handler_instance: object
        if isinstance(new_handler_kwarg, dict):
            handler_instance=handler_class(**new_handler_kwarg)
        else:
            handler_instance = new_handler_kwarg
        del new_handler_kwarg
        handler_class = type(handler_instance)

        messages_in_handler_class: Dict[Type["RdisqMessage"], Callable] = {}
        for m, f in self._messages_registered_handlers.items():
            if self._get_handler_class_for_function(f) == handler_class:
                messages_in_handler_class[m] = f
        if not messages_in_handler_class:
            raise RuntimeError(
                f"Tried registering messages of {handler_instance}, but it had no registered message handlers")
        else:
            result: Dict[Type[RdisqMessage], _Handler] = {}
            for m, f in messages_in_handler_class.items():
                result[m] = self.create_handler(m, handler_instance)
            return result

    @staticmethod
    def _get_handler_class_for_function(handler_function: Callable):
        path = handler_function.__qualname__.split('.')
        # noinspection PyUnresolvedReferences
        module = import_module(handler_function.__module__)
        try:
            handler_class = getattr(module, path[-2])
        except IndexError:
            handler_class = None
        else:
            if not isinstance(handler_class, type):
                raise RuntimeError(
                    f"Could not determine if {handler_function} is"
                    f" a bound-method or a standalone function.")

        return handler_class
