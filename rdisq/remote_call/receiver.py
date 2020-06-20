from typing import *
from functools import partial

from rdisq.redis_dispatcher import PoolRedisDispatcher
from rdisq.remote_call.message import RdisqMessage
from rdisq.service import RdisqService, remote_method


class StartHandlingMessages(RdisqMessage):
    def __init__(self, new_message_class: Type[RdisqMessage]):
        self.new_message_class = new_message_class
        super().__init__()


class StopHandlingMessages(RdisqMessage):
    def __init__(self, old_message_class: Type[RdisqMessage]):
        self.old_message_class = old_message_class
        super().__init__()


class ReceiverService(RdisqService):
    service_name = "ReceiverService"
    response_timeout = 10  # seconds
    redis_dispatcher = PoolRedisDispatcher(host='127.0.0.1', port=6379, db=0)

    @StartHandlingMessages.set_handler
    def register_message(self, new_message_class: Type[RdisqMessage], instance: Optional[object] = None):
        self.register_method_to_queue(
            partial(
                self.run_handler, message_class=new_message_class,
                handler_instance=instance),
            new_message_class.get_message_class_id()
        )
        return True

    @StopHandlingMessages.set_handler
    def unregister_message(self, old_message_class: Type[RdisqMessage]):
        self.unregister_from_queue(old_message_class.get_message_class_id())

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

        self.register_message(StartHandlingMessages, self)
        self.register_message(StopHandlingMessages, self)
