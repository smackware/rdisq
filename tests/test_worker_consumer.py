# https://www.python.org/dev/peps/pep-0366/
if __name__ == "__main__" and __package__ is None:
    __package__ = "tests"

import threading
from unittest.mock import Mock, patch
import pytest
from examples.simple.worker import SimpleWorker, GrumpyException
from examples.complex.complex_worker import ComplexWorker


@pytest.fixture
def simple_worker():
    _worker = SimpleWorker()
    _processor = threading.Thread(
        group=None,
        target=_worker.process
    )
    _processor.start()
    yield _worker
    _worker.stop()


@pytest.fixture
def complex_worker():
    _worker = ComplexWorker()
    _processor = threading.Thread(
        group=None,
        target=_worker.process
    )
    _processor.start()
    yield _worker
    _worker.stop()


def test_simple_positive(simple_worker):
    assert SimpleWorker.get_consumer().add(1, 2) == 3
    assert (
        SimpleWorker.get_consumer().build("a house"),
        {'message from the worker': "I'm done!"}

    )
    assert (
        SimpleWorker.get_consumer().build("a house", "hammer"),
        {'message from the worker': "I'm done!"}
    )


def test_simple_negative(simple_worker):
    try:
        SimpleWorker.get_consumer().add(1, 2, 3)
        raise
    except TypeError:
        pass

    try:
        SimpleWorker.get_consumer().grumpy()
        raise
    except GrumpyException:
        pass


def test_complex_positive(complex_worker):
    consumer = ComplexWorker.get_consumer()
    async_consumer = ComplexWorker.get_async_consumer()
    assert consumer.add_log("Going to calculate!") == "Going to calculate!"
    async = async_consumer.calculate(1, 2, 3)
    assert async.wait() == ComplexWorker.calculate(1, 2, 3)
    # # If we go async, we can tell the processing time and the total roundtrip time
    # consumer.add_log("Got: %d, Processed in %f seconds, total seconds: %f" % (
    #     result, async.process_time_seconds, async.total_time_seconds,))
