from rdisq import Rdisq
from rdisq.config import SimpleQueueConfig

class GrumpyException(Exception):
    pass

class MyClass(Rdisq):
    def q_add(self, a, b):
        # Do some simple work
        return a + b

    def q_build(self, what, tool=None):
        # Showing here that args and kwargs can be used
        if tool is not None:
            print "I built you %s, using a %s" % (what, tool,)
        else:
            print "I built you %s, using a my bear (sic) hands" % (what, )
    
        # Return a dict, just to spice things up a bit
        return {"message from the worker": "I'm done!"}

    def q_grumpy(self):
        raise GrumpyException("I'M ALWAYS GRUMPY!")


myClass = MyClass(SimpleQueueConfig("queue_prefix"))
if __name__ == '__main__':
    myClass.process()
