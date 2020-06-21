from typing import *
from rdisq.remote_call.message import RdisqMessage
from rdisq.remote_call.receiver import ReceiverService

if TYPE_CHECKING:
    from rdisq.response import RdisqResponse


class Call:
    message: RdisqMessage
    _sent: bool = False
    _response: "RdisqResponse"

    def __init__(self, message: RdisqMessage):
        self.message = message

    @property
    def response(self)->"RdisqResponse":
        if self._sent:
            return self._response
        else:
            raise RuntimeError("not supposed to try to get results before the message was sent")

    def send(self, timeout=None)->Any:
        """
        Send the message and wait for a reply.

        :param timeout: How long to wait for reply .
        :return: The returned value of the remote method.
        """
        self.send_async()
        return self._response.wait(timeout)

    def send_async(self)->"RdisqResponse":
        assert not self._sent, "This message has already been sent"
        self._sent = True
        self._response = ReceiverService.get_consumer().send(
            self.message.get_message_class_id(),
            **self.message.__dict__
        )
        return self.response
