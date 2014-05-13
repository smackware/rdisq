__author__ = 'smackware'


try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps


def encode(obj):
    return dumps(obj)


def decode(data):
    return loads(data)