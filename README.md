rdisq
======

Super minimal framework for working with redis worker/consumer queues for distributed apps.


examples
==========
Please see the examples dir


Quick but full start
==========
- Install redis (apt-get install redis, or yum, or... well... you get it :)
- Install redis python module 
- Install this module
- Write a simple worker (worker.py)
```
from rdisq.service import RdisqService
from rdisq.redis_dispatcher import PoolRedisDispatcher


class MyClass(RdisqService):
    service_name = "my_service"
    response_timeout = 10 # seconds
    redis_dispatcher = PoolRedisDispatcher(host='localhost', port=6379, db=0)

    def q_do_work(self, param1, param2, param3=None):
        # Just return a simple dict, but technically we can do w/e we like here
        data = {
            "first": param1,
            "second": param2,
            "key_arg": param3,
            }
        return data

    def q_add_num(self, a, b):
        return a + b

# We can instantiate our worker right away
# Since this is an example, lets also start the blocking processing loop here
if __name__ == '__main__':
    MyWorker().process() # Blocking loop
    
```

Get the remote consumer inside another python process

```
from worker import MyClass

# NOTICE: we omitted the 'q_' prefix of the method
print MyClass.get_consumer().do_work("p1", "sasfas", param3="a")  # prints '''{"first":"p1", "seconds":"sasfas", "key_arg":"a"}'''

# We can also call the async one and get a callback object
response = MyClass.get_async_consumer().do_work("p1", "sasfas") # returns a Response object
print response.wait() # blocks until a response (or a timeout), prints '''{"first":"p1", "seconds":"sasfas", "key_args":None}'''

```
