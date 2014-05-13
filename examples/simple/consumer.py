#!/usr/bin/env python

from worker import GrumpyException
from worker import MyClass


# Call the add method
print MyClass.get_consumer().add(1, 2)

# call the build method
print MyClass.get_consumer().build("a house")

# Now we will call the async form of "build"
result = MyClass.get_async_consumer().build("a house", "hammer")
print result.wait()

try:
    result = MyClass.get_consumer().grumpy()
except GrumpyException as e:
    print "he's grumpy"
