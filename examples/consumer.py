#!/usr/bin/env python

from worker import myClass

# Call the add method
print myClass.add(1,2)

# call the build method
print myClass.build("a house")

# Now we will call the async form of "build"
result = myClass.async_build("a house", "hammer")
print result.wait()
