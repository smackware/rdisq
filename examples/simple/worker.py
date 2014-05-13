from redis import Redis
from rdisq import Rdisq


class GrumpyException(Exception):
    pass


class MyClass(Rdisq):
    queue_name = "my_service"
    queue_timeout = 10 # seconds

    def get_redis(self):
        return Redis()

    @staticmethod
    def q_add(a, b):
        # Do some simple work
        return a + b

    def q_build(self, what, tool=None):
        # Showing here that args and kwargs can be used
        if tool is not None:
            print "%s: I built you %s, using a %s" % (self.queue_name, what, tool,)
        else:
            print "%s: I built you %s, using a my bear [sic] hands" % (self.queue_name, what, )
    
        # Return a dict, just to spice things up a bit
        return {"message from the worker": "I'm done!"}

    @staticmethod
    def q_grumpy():
        raise GrumpyException("I'M ALWAYS GRUMPY!")



if __name__ == '__main__':
    myClass = MyClass()
    myClass.process()
