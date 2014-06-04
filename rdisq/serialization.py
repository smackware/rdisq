__author__ = 'smackware'


try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps


class AbstractSerializer(object):
    def dumps(self, obj):
        raise NotImplementedError("encode is not implemented")

    def loads(self, obj):
        raise NotImplementedError("decode is not implemented")


class PickleSerializer(object):
    @staticmethod
    def dumps(obj):
        return dumps(obj)

    @staticmethod
    def loads( data):
        return loads(data)