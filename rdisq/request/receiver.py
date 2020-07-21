from typing import *

from rdisq.consts import RECEIVER_SERVICE_NAME
from rdisq.configuration import get_rdisq_config
from rdisq.payload import SessionResult
from rdisq.request.message import RdisqMessage
from rdisq.request.dispatcher import RequestDispatcher
from rdisq.service import RdisqService, remote_method

from rdisq.request.handler import _Handler


class RegisterMessage(RdisqMessage):
    def __init__(self, new_message_class: Type[RdisqMessage], new_handler_kwargs: Union[Dict, object] = None):
        """
        :param new_message_class: The receiver will start handling messages of this class
        :param new_handler_kwargs: A kwargs dictionary for the new handler, or a new handler
        """
        self.new_message_class = new_message_class
        self.new_handler_instance = new_handler_kwargs
        super().__init__()


class UnregisterMessage(RdisqMessage):
    def __init__(self, old_message_class: Type[RdisqMessage]):
        self.old_message_class = old_message_class
        super().__init__()


class GetRegisteredMessages(RdisqMessage):
    def __init__(self):
        super().__init__()


class RegisterAll(RdisqMessage):
    def __init__(self, new_handler_kwargs: Union[Dict, object] = None, handler_class: type = None):
        super(RegisterAll, self).__init__()
        self.handler_class = handler_class
        self.new_handler_kwargs = new_handler_kwargs


class AddQueue(RdisqMessage):
    def __init__(self, new_queue_name: str):
        self.new_queue_name = new_queue_name
        super().__init__()


class RemoveQueue(RdisqMessage):
    def __init__(self, old_queue_name: str):
        self.old_queue_name = old_queue_name
        super(RemoveQueue, self).__init__()


class GetStatus(RdisqMessage):
    def __init__(self):
        super(GetStatus, self).__init__()


class SetReceiverTags(RdisqMessage):
    def __init__(self, new_dict: Dict):
        super().__init__()
        self.new_dict = new_dict


CORE_RECEIVER_MESSAGES = {RegisterMessage, UnregisterMessage, GetRegisteredMessages, AddQueue, RemoveQueue,
                          RegisterAll, SetReceiverTags}


class ReceiverService(RdisqService):
    service_name = RECEIVER_SERVICE_NAME
    response_timeout = 10  # seconds
    redis_dispatcher: RequestDispatcher
    _handlers: Dict[Type[RdisqMessage], "_Handler"]
    _tags: Dict = None

    def __init__(self, uid=None, message_class: Type[RdisqMessage] = None, instance: object = None,
                 dispatcher: RequestDispatcher = None):
        self._tags = {}
        self.redis_dispatcher = dispatcher or get_rdisq_config().request_dispatcher
        super().__init__(uid)
        self._handlers = dict()

        for m in CORE_RECEIVER_MESSAGES:
            handling_message = RegisterMessage(m, self)
            self.register_message(handling_message)

        if message_class:
            self.register_message(RegisterMessage(message_class, instance))

        self._on_process_loop()

    @property
    def tags(self) -> Dict:
        return self._tags.copy()

    @tags.setter
    def tags(self, value: Dict):
        if not isinstance(value, dict):
            raise RuntimeError("tags must be a dict")
        else:
            self._tags = value

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
    def get_registered_messages(
            self, message: GetRegisteredMessages = None) -> Set[Type[RdisqMessage]]:
        f"""{message} is present so as not to break uniformity, but isn't used."""
        return set(self._handlers.keys())

    @RegisterMessage.set_handler
    def register_message(self, message: RegisterMessage) -> Set[Type[RdisqMessage]]:
        if message.new_message_class in self.get_registered_messages():
            raise RuntimeError(
                f"Tried registering {message.new_message_class} to {self}."
                f"But it's already registered."
            )

        self.add_queue(AddQueue(message.new_message_class.get_message_class_id()))
        self._handlers[message.new_message_class] = get_rdisq_config().handler_factory.create_handler(
            message.new_message_class, message.new_handler_instance, self._handlers.values())

        self._on_process_loop()
        return self.get_registered_messages()

    @RegisterAll.set_handler
    def register_all(self, message: RegisterAll) -> Set[Type[RdisqMessage]]:
        handlers: Dict[Type[RdisqMessage], "_Handler"] = get_rdisq_config().handler_factory.create_handlers_for_object(
            message.new_handler_kwargs, message.handler_class)
        for message_type, handler in handlers.items():
            self.add_queue(AddQueue(message_type.get_message_class_id()))
            self._handlers[message_type] = handler
        self._on_process_loop()
        return self.get_registered_messages()

    @UnregisterMessage.set_handler
    def unregister_message(self, message: UnregisterMessage):
        self.unregister_from_queue(message.old_message_class.get_message_class_id())
        self._handlers.pop(message.old_message_class)

        self._on_process_loop()
        return self.get_registered_messages()

    @SetReceiverTags.set_handler
    def set_tags(self, message: SetReceiverTags) -> Dict:
        self.tags = message.new_dict
        self._on_process_loop()
        return self.tags

    @remote_method
    def receive_message(self, message: RdisqMessage):
        if type(message) not in self.get_registered_messages():
            raise RuntimeError(f"Received an unregistered message {type(message)}")
        handler_result = self._handlers[type(message)].handle(message)
        if message.session_data is not None:
            result = SessionResult(result=handler_result, session_data=message.session_data)
        else:
            result = handler_result
        return result

    def _on_process_loop(self):
        self.redis_dispatcher.update_receiver_service_status(self)
