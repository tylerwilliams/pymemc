class MemcachedError(Exception):
    pass

class MemcachedConnectionClosedError(MemcachedError):
    pass