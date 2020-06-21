from typing import *
from functools import partial

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.remote_call.message import RdisqMessage
from rdisq.service import RdisqService, remote_method


class StartHandling(RdisqMessage):
    def __init__(self, new_message_class: Type[RdisqMessage], new_handler_kwargs: Dict = None):
        """
        :param new_message_class: The worker will start handling messages of this class
        :param new_handler_kwargs: A kwargs dictionary for the new handler
        """
        self.new_message_class = new_message_class
        self.new_handler_instance = new_handler_kwargs
        super().__init__()


class StopHandling(RdisqMessage):
    def __init__(self, old_message_class: Type[RdisqMessage]):
        self.old_message_class = old_message_class
        super().__init__()


class GetRegisteredMessages(RdisqMessage):
    def __init__(self):
        super().__init__()


class ReceiverService(RdisqService):
    service_name = "ReceiverService"
    response_timeout = 10  # seconds
    redis_dispatcher = PoolRedisDispatcher(host='127.0.0.1', port=6379, db=0)

    @GetRegisteredMessages.set_handler
    def get_registered_messages(self) -> Set[RdisqMessage]:
        handlers: FrozenSet[partial] = self.callables
        message_classes: Set[RdisqMessage] = set()
        for handler in handlers:
            if isinstance(handler, partial):
                message_class = handler.keywords.get("message_class")
                if message_class:
                    message_classes.add(message_class)

        return message_classes

    @StartHandling.set_handler
    def register_message(self, new_message_class: Type[RdisqMessage],
                         new_handler_instance: Optional[Union[object, Dict]] = None, ):
        if isinstance(new_handler_instance, dict):
            new_handler_instance = new_message_class.spawn_handler_instance(**new_handler_instance)
        self.register_method_to_queue(
            partial(
                self.run_handler, message_class=new_message_class,
                handler_instance=new_handler_instance),
            new_message_class.get_message_class_id()
        )
        return self.get_registered_messages()

    @StopHandling.set_handler
    def unregister_message(self, old_message_class: Type[RdisqMessage]):
        self.unregister_from_queue(old_message_class.get_message_class_id())
        return self.get_registered_messages()

    @remote_method
    def run_handler(self, message_class: RdisqMessage, handler_instance: object, *args, **kwargs):
        return message_class.call_handler(
            instance=handler_instance, *args, **kwargs)

    def __init__(self, uid=None,
                 message_class: Type[RdisqMessage] = None,
                 instance: object = None
                 ):
        super().__init__(uid)
        if message_class:
            self.register_message(message_class, instance)

        self.register_message(StartHandling, self)
        self.register_message(StopHandling, self)
        self.register_message(GetRegisteredMessages, self)
