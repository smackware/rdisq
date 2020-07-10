from rdisq.request.message import RdisqMessage


class SumMessage(RdisqMessage):
    def __init__(self, first: int, second: int):
        self.first = first
        self.second = second
        super(SumMessage, self).__init__()


@SumMessage.set_handler
def sum_(message: SumMessage):
    return message.first + message.second


class AddMessage(RdisqMessage):
    def __init__(self, new: int) -> None:
        self.new = new
        super().__init__()


class SubtractMessage(RdisqMessage):
    def __init__(self, subtrahend: int) -> None:
        self.subtrahend = subtrahend
        super().__init__()


class Summer:
    def __init__(self, start: int = 0):
        self.sum = start

    @AddMessage.set_handler
    def add(self, message: AddMessage):
        self.sum += message.new
        return self.sum

    @SubtractMessage.set_handler
    def subtract(self, message: SubtractMessage):
        self.sum -= message.subtrahend
        return self.sum