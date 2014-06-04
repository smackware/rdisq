import logging
from rdisq.service import RdisqService, remote_method
from rdisq.redis_dispatcher import PoolRedisDispatcher


class Worker(RdisqService):
    logger = logging.getLogger(__name__)
    log_returned_exceptions = True
    response_timeout = 5
    stop_on_fail = False
    redis_dispatcher = PoolRedisDispatcher(host='localhost', port=6379, db=0)

    @staticmethod
    @remote_method
    def calculate(a, b, c):
        return (a*b) + c;

    @remote_method
    def add_log(self, log_line):
        # A very crude way to log :) just for the sake of the example
        print log_line 

    def on_start(self):
        print "Service started: %s!" % (self.service_name, )
    
    def pre(self, q):
        print "Processing from %s" % (q, )

    def post(self, q):
        print "Finished processing from %s" % (q, )

if __name__ == '__main__':
    Worker().process()
