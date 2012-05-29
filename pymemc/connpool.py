import Queue
import socket
import contextlib
import functools

@contextlib.contextmanager
def pooled_connection(pool):
    conn = pool.get()
    try:
        yield conn
    except Exception:
        raise
    else:
        pool.put(conn)

def reconnect(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        retries = kwargs.pop('retries', 1)
        tries = 0
        
        while True:
            try:
                return method(*args, **kwargs)
            except Exception:
                if tries >= retries:
                    raise
                tries += 1

    return wrapper        

class ConnectionPool(object):
    def __init__(self, klass, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._queue = Queue.Queue(self._kwargs.pop('pool_size', 5))
        self._klass = klass

    def get(self):
        try:
            return self._queue.get_nowait()
        except Queue.Empty:
            return self._klass(*self._args, **self._kwargs)

    def put(self, conn):
        try:
            self._queue.put_nowait(conn)
        except Queue.Full:
            pass
                    
class SocketConnectionPool(ConnectionPool):
    def __init__(self, *args, **kwargs):
        def socket_create_and_connect(*args, **kwargs):
            sock = socket.socket()
            sock.connect(*args, **kwargs)
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            return sock
        super(SocketConnectionPool, self).__init__(socket_create_and_connect, *args, **kwargs)
