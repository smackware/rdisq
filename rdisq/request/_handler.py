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

    def __init__(self, handler_function: Callable):
        self._handler_function = handler_function

    def spawn_handler(self,
                      instance_param: Union[Dict, object] = None,
                      sibling_handlers: Iterable[_Handler] = frozenset()) -> _Handler:
        """
        :param instance_param: If the function is a method, this is an instance of the method's class, or kwargs for making a new one.
        If the function is a method but the instance is None, will attempt to reuse an instance from a sibling.
        :param sibling_handlers: Other handlers whose instances the new handler can reuse.
        """
        return _Handler(
            self._handler_function,
            self._handler_class, instance_param, sibling_handlers)

    @property
    def _handler_class(self) -> Optional[Type]:
        """
        :return: If this message's handler is an bound-method, then return the class that contains it.
        """
        path = self._handler_function.__qualname__.split('.')
        # noinspection PyUnresolvedReferences
        module = import_module(self._handler_function.__module__)
        try:
            handler_class = getattr(module, path[-2])
        except IndexError:
            handler_class = None
        else:
            if not isinstance(handler_class, type):
                raise RuntimeError(
                    f"Could not determine if {self._handler_function} is"
                    f" a bound-method or a standalone function.")

        return handler_class
