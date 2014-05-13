__author__ = 'smackware'

import os
import uuid


def get_mac():
    return uuid.getnode()


def get_consumer_id():
    return "%s-%s" % (get_mac(), os.getpid(), )


def get_request_key(task_id):
    return "request_%s" % (task_id, )


def generate_task_id():
    return "%s-%s" % (get_consumer_id(), uuid.uuid4().hex, )


