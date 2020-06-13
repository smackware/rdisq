from rdisq.service import RdisqService, remote_method
from rdisq.redis_dispatcher import PoolRedisDispatcher


class GrumpyException(Exception):
    pass


class SimpleWorker(RdisqService):
    service_name = "MyClass"
    response_timeout = 10 # seconds
    redis_dispatcher = PoolRedisDispatcher(host='127.0.0.1', port=6379, db=0)


    @staticmethod
    @remote_method
    def add(a, b):
        # Do some simple work
        return a + b

    @remote_method
    def build(self, what, tool=None):
        # Showing here that args and kwargs can be used
        if tool is not None:
            print("%s: I built you %s, using a %s" % (self.service_name, what, tool,))
        else:
            print("%s: I built you %s, using a my bear [sic] hands" % (self.service_name, what, ))
    
        # Return a dict, just to spice things up a bit
        return {"message from the worker": "I'm done!"}

    @staticmethod
    @remote_method
    def grumpy():
        raise GrumpyException("I'M ALWAYS GRUMPY!")


if __name__ == '__main__':
    myClass = SimpleWorker()
    myClass.process()
