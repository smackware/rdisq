from rdisq.service import RdisqService
from rdisq.redis_dispatcher import PoolRedisDispatcher


class Worker(RdisqService):
    service_name = "BestServiceEver"
    response_timeout = 5
    stop_on_fail = False
    redis_dispatcher = PoolRedisDispatcher(host='localhost', port=6379, db=0)

    def q_calculate(self, a, b, c):
        return (a*b) + c;

    def q_add_log(self, log_line):
        # A very crude way to log :) just for the sake of the example
        print log_line 
    
    def on_start(self):
        print "Service started!"
    
    def pre(self, q):
        print "Processing from %s" % (q, )

    def post(self, q):
        print "Finished processing from %s" % (q, )

    def on_exception(self, e):
        # If we dont re-raise here, we will continue processing
        print "ERROR: " + str(e)
        if self.stop_on_fail:
            raise e

if __name__ == '__main__':
    Worker().process()
