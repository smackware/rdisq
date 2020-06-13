#!/usr/bin/env python

# https://www.python.org/dev/peps/pep-0366/
if __name__ == "__main__" and __package__ is None:
    __package__ = "examples.simple"

print("starting import")
from .worker import GrumpyException
from .worker import SimpleWorker

# Call the add method
print(SimpleWorker.get_consumer().add(1, 2))

# call the build method
print(SimpleWorker.get_consumer().build("a house"))

# Now we will call the async form of "build"
result = SimpleWorker.get_async_consumer().build("a house", "hammer")
print(result.wait())

try:
    result = SimpleWorker.get_consumer().grumpy()
except GrumpyException as e:
    print("he's grumpy")
