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
- Write a simple service (worker.py)

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
- Lets invoke the service's blocking loop so it can start to process.
```
from worker import MyService
if __name__ == '__main__':
    MyService().process() # Blocking loop
```
- In another python interpreter, we can call the remote method
```
from worker import MyService

consumer = MyService.get_consumer()
returned = consumer.do_work("Foo", "Bar", param3="Pow") # will return the string "Foo, Bar and Pow"
```
- If wanted, an async call can also be made
```
from worker import MyService

consumer = MyService.get_async_consumer()
async_response = consumer.do_work("Foo", "Bar", param3="Pow") # will return an async response object

if async_response.is_processed():
    print "We've got a response back!"
    if async_response.is_exception():
        print "... but it was an exception :/"
        raise async_response.exception

async_respones.wait() # will auto-raise the remote exception if one was raise in the remote_method's body.
```