from typing import *

from rdisq.consts import RECEIVER_SERVICE_NAME
from rdisq.request.message import RdisqMessage
from rdisq.request.dispatcher import RequestDispatcher
from rdisq.service import RdisqService, remote_method


class StartHandling(RdisqMessage):
    def __init__(self, new_message_class: Type[RdisqMessage], new_handler_kwargs: Union[Dict, object] = None):
        """
        :param new_message_class: The receiver will start handling messages of this class
        :param new_handler_kwargs: A kwargs dictionary for the new handler, or a new handler
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


class AddQueue(RdisqMessage):
    def __init__(self, new_queue_name: str):
        self.new_queue_name = new_queue_name
        super().__init__()


class RemoveQueue(RdisqMessage):
    def __init__(self, old_queue_name: str):
        self.old_queue_name = old_queue_name
        super(RemoveQueue, self).__init__()


CORE_RECEIVER_MESSAGES = {StartHandling, StopHandling, GetRegisteredMessages, AddQueue, RemoveQueue}


class ReceiverService(RdisqService):
    service_name = RECEIVER_SERVICE_NAME
    response_timeout = 10  # seconds
    redis_dispatcher = RequestDispatcher(host='127.0.0.1', port=6379, db=0)
    _message_to_handler_instance: Dict[Type[RdisqMessage], Union[object, None]]

    def __init__(self, uid=None, message_class: Type[RdisqMessage] = None, instance: object = None):
        super().__init__(uid)
        self._message_to_handler_instance = dict()
        if message_class:
            self.register_message(StartHandling(message_class, instance))

        for m in CORE_RECEIVER_MESSAGES:
            self.register_message(StartHandling(m, self))

        self._on_process_loop()

    @AddQueue.set_handler
    def add_queue(self, message: AddQueue):
        self.register_method_to_queue(self.receive_message, message.new_queue_name)
        self._on_process_loop()
        return self.listening_queues

    @RemoveQueue.set_handler
    def remove_queue(self, message: RemoveQueue):
        self.unregister_from_queue(message.old_queue_name)
        self._on_process_loop()
        return self.listening_queues

    @GetRegisteredMessages.set_handler
    def get_registered_messages(self, message: GetRegisteredMessages = None) -> Set[Type[RdisqMessage]]:
        return set(self._message_to_handler_instance.keys())

    @StartHandling.set_handler
    def register_message(self, message: StartHandling) -> Set[Type[RdisqMessage]]:
        if message.new_message_class in self.get_registered_messages():
            raise RuntimeError(
                f"Tried registering {message.new_message_class} to {self}, but it's already registered."
            )

        new_handler_instance = None
        if isinstance(message.new_handler_instance, dict):
            new_handler_instance = message.new_message_class.spawn_handler_instance(
                **message.new_handler_instance)
        elif message.new_message_class.get_handler_class() and isinstance(message.new_handler_instance,
                                                                          message.new_message_class.get_handler_class()):
            new_handler_instance = message.new_handler_instance
        elif message.new_handler_instance is None:
            if message.new_message_class.get_handler_class() is None:
                new_handler_instance = None
            else:
                for k, v in self._message_to_handler_instance.items():
                    if type(v) == message.new_message_class.get_handler_class():
                        new_handler_instance = v
                        break
                if not new_handler_instance:
                    raise RuntimeError(f"Did not provide handler instance")
        else:
            raise RuntimeError(f"invalid handler instance {message.new_handler_instance}")

        self.add_queue(AddQueue(message.new_message_class.get_message_class_id()))
        self._message_to_handler_instance[message.new_message_class] = new_handler_instance

        self._on_process_loop()
        return self.get_registered_messages()

    @StopHandling.set_handler
    def unregister_message(self, message: StopHandling):
        self.unregister_from_queue(message.old_message_class.get_message_class_id())
        self._message_to_handler_instance.pop(message.old_message_class)

        self._on_process_loop()
        return self.get_registered_messages()

    @remote_method
    def receive_message(self, message: RdisqMessage):
        if type(message) not in self.get_registered_messages():
            raise RuntimeError(f"Received an unregistered message {type(message)}")
        return message.call_handler(
            message,
            instance=self._message_to_handler_instance[type(message)])

    def _on_process_loop(self):
        self.redis_dispatcher.update_receiver_service_status(self)
