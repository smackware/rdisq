from typing import *

if TYPE_CHECKING:
    from rdisq.request.dispatcher import RequestDispatcher
    from rdisq.request.handler import _HandlerFactory


class _RdisqConfig:
    _handler_factory: "_HandlerFactory" = None
    _dispatcher: "RequestDispatcher" = None
    _default_config: ClassVar["_RdisqConfig"] = None

    @property
    def handler_factory(self) -> "_HandlerFactory":
        from rdisq.request.handler import _HandlerFactory
        if not self._handler_factory:
            self._handler_factory = _HandlerFactory()
        return self._handler_factory

    @property
    def request_dispatcher(self) -> "RequestDispatcher":
        from rdisq.request.dispatcher import RequestDispatcher
        if not self._dispatcher:
            self._dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
        return self._dispatcher

    @classmethod
    def get_default_config(cls):
        if not cls._default_config:
            cls._default_config = _RdisqConfig()
        return cls._default_config

def get_rdisq_config() -> _RdisqConfig:
    return _RdisqConfig.get_default_config()
