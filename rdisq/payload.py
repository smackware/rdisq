__author__ = 'smackware'

from collections import namedtuple

RequestPayload = namedtuple("RequestPayload", "task_id timeout args kwargs")
ResponsePayload = namedtuple("ResponsePayload", "returned_value raised_exception processing_time_seconds")