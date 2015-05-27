from functools import wraps
import logging
import time

class EtcdError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class CurrentLeaderError(EtcdError):
    pass


class HealthiestMemberError(EtcdError):
    pass

def _retry(func, retries, errors, default):
    @wraps(func)
    def wrapped(*args, **kwargs):
        for i in range(retries):
            if i != 0:
                time.sleep( 2 ** (i + 1) )

            ex = None
            try:
                value = func()
            except errors as e:
                logging.exception(func.__name__)
                ex = e
            else:
                if value:
                    return value

        if ex:
            raise ex
        if default:
            raise default
    return wrapped

def retry(retries, errors=Exception, default=None):
    if isinstance(default, str):
        default = Exception(default)
    return (lambda func: _retry(func, retries, errors, default))
