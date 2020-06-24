from typing import *

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.remote_call.message import RdisqMessage
from rdisq.remote_call.message_dispatcher import MessageDispatcher
from rdisq.service import RdisqService, remote_method


class StartHandling(RdisqMessage):
    def __init__(self, new_message_class: Type[RdisqMessage], new_handler_kwargs: Dict = None):
        """
        :param new_message_class: The receiver will start handling messages of this class
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
    redis_dispatcher = MessageDispatcher(host='127.0.0.1', port=6379, db=0)
    _message_to_handler_instance: Dict[Type[RdisqMessage], Union[object, None]]

    def __init__(self, uid=None, message_class: Type[RdisqMessage] = None, instance: object = None):
        super().__init__(uid)
        self._message_to_handler_instance = dict()
        if message_class:
            self.register_message(message_class, instance)

        self.register_message(StartHandling, self)
        self.register_message(StopHandling, self)
        self.register_message(GetRegisteredMessages, self)

    @GetRegisteredMessages.set_handler
    def get_registered_messages(self) -> Set[Type[RdisqMessage]]:
        return set(self._message_to_handler_instance.keys())

    @StartHandling.set_handler
    def register_message(self, new_message_class: Type[RdisqMessage],
                         new_handler_instance: Optional[Union[object, Dict]] = None, ) -> Set[Type[RdisqMessage]]:
        """
        Ask this message-receiver to start receiving a new class of messages.

        :param new_message_class:
        :param new_handler_instance: If this is a dict, it'll be used as kwargs to handler factory.
        :return: supported messages after the update
        """
        if new_message_class in self.get_registered_messages():
            raise RuntimeError(f"Tried registering {new_message_class} to {self}, but it's already registered.")

        if isinstance(new_handler_instance, dict):
            new_handler_instance = new_message_class.spawn_handler_instance(**new_handler_instance)

        self.register_method_to_queue(self.receive_message, new_message_class.get_message_class_id())
        self._message_to_handler_instance[new_message_class] = new_handler_instance
        return self.get_registered_messages()

    @StopHandling.set_handler
    def unregister_message(self, old_message_class: Type[RdisqMessage]):
        self.unregister_from_queue(old_message_class.get_message_class_id())
        self._message_to_handler_instance.pop(old_message_class)
        return self.get_registered_messages()

    @remote_method
    def receive_message(self, message: RdisqMessage):
        if type(message) not in self.get_registered_messages():
            raise RuntimeError(f"Received an unregistered message {type(message)}")
        return message.call_handler(
            instance=self._message_to_handler_instance[type(message)], **message.__dict__)

    def _on_process_loop(self):
        self.redis_dispatcher.update_receiver_service_status(self)
