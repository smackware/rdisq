from typing import *
from threading import Thread

from rdisq.event import RdisqEvent, EventWorker


class AddEvent(RdisqEvent):
    """Event for adding two numbers"""
    first: int
    second: int

    def __init__(self, first: int, second: int):
        super(AddEvent, self).__init__(first=first, second=second)


@AddEvent.set_handler
def add(first, second):
    return first + second


def test_event():
    event_worker = EventWorker(event_class=AddEvent)
    Thread(group=None, target=event_worker.process).start()

    event = AddEvent(1, 2)
    assert event.send() == add(1, 2)

    event = AddEvent(3, 2)
    assert event.send() == add(3, 2)

    event_worker.stop()


# =============================================

class SumEvent(RdisqEvent):
    def __init__(self, new: float):
        self.new = new
        super(SumEvent, self).__init__()


class Summer:
    def __init__(self):
        self.sum = 0

    @SumEvent.set_handler
    def add(self, new):
        self.sum += new
        return self.sum


def test_class_event():
    summer = Summer()
    event_worker = EventWorker(event_class=SumEvent, instance=summer)
    Thread(group=None, target=event_worker.process).start()

    event = SumEvent(new=1)
    assert event.send() == 1

    try:
        event.send() == 1
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed event ruse")

    event = SumEvent(new=2)
    try:
        event.result
    except:
        pass
    else:
        raise RuntimeError("Should not have allowed getting result before the evnet has run")


    assert event.send() == 3

    assert summer.sum == 3
    assert event.result == 3
    event_worker.stop()
