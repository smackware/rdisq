from rdisq.redis_dispatcher import LocalRedisDispatcher
from rdisq.service import RdisqService


class GrumpyException(Exception):
    pass


class MyClass(RdisqService):
    service_name = "my_service"
    queue_timeout = 10 # seconds
    redis_dispatcher = LocalRedisDispatcher()

    @staticmethod
    def q_add(a, b):
        # Do some simple work
        return a + b

    def q_build(self, what, tool=None):
        # Showing here that args and kwargs can be used
        if tool is not None:
            print "%s: I built you %s, using a %s" % (self.service_name, what, tool,)
        else:
            print "%s: I built you %s, using a my bear [sic] hands" % (self.service_name, what, )
    
        # Return a dict, just to spice things up a bit
        return {"message from the worker": "I'm done!"}

    @staticmethod
    def q_grumpy():
        raise GrumpyException("I'M ALWAYS GRUMPY!")



if __name__ == '__main__':
    myClass = MyClass()
    myClass.process()
