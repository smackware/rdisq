EXPORTED_METHOD_PREFIX = "q_"


def remote_method(callable):
    callable.is_remote = True
    return callable