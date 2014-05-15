rdisq
======

Super minimal framework for working with redis worker/consumer queues for distributed apps.


examples
==========
Please see the examples dir


Quick but full start
==========

Installation
-----------
1. Install a redis-server
2. pip install redis
3. pip install git+https://github.com/smackware/rdisq@master#egg=rdisq

Writing a simple service
-----------
```
from rdisq.service import RdisqService, remote_method
from rdisq.redis_dispatcher import PoolRedisDispatcher

class MyService(RdisqService):
    service_name = "my_service"
    response_timeout = 10 # seconds
    redis_dispatcher = PoolRedisDispatcher(host='localhost', port=6379, db=0)

    @remote_method
    def do_work(self, param1, param2, param3=None):
        # Add your code here, return normally as if its the same program
        return "%s, %s and %s" % (param1, param2, param3)
```

Invoking a service
-----------

- Lets invoke the service's blocking loop so it can start to process.
```
bash$ ipython

> from worker import MyService
> MyService().process() # Blocking loop
```

The service is now active and is ready to process requests.
FYI - you can invoke as many of those as you'd like. On the local computer or on a remote one
as long as redis connectivity is possible.

Using the remote methods from another python process
-----------
- In another python interpreter, we can call the remote method
```
from worker import MyService

consumer = MyService.get_consumer()
returned = consumer.do_work("Foo", "Bar", param3="Pow") # will return the string "Foo, Bar and Pow"
```

A remote method is processed on a service worker of the consumer's class (MyService or any class inheriting it)
You have no control over which worker of the same service class will process your call. The first one available for
processing will jump on the opportunity.

If you wish control over which worker does what, I recommend creating different service classes and using their consumers

Asynchronous remote method
-----------

All of the remote methods can be used asynchronously via the "async_consumer" instance

```
# stdlib imports
from time import sleep

# app imports
from worker import MyService

consumer = MyService.get_async_consumer()
async_response = consumer.do_work("Foo", "Bar", param3="Pow") # will return an async response object

sleep(1)

if async_response.is_processed():
    print "We've got a response back!"

# We still need to call ".wait()" to process the response data.
async_respones.wait() # will auto-raise the remote exception if one was raise in the remote_method's body.
```