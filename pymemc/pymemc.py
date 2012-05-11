import struct
import logging
import collections
import contextlib
import pkg_resources
import itertools

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle

import chash
import threadpool
import connpool

try:
    __version__ = pkg_resources.require("pymemc")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "0.0.0"

__all__ = [
    'Client',
    'MemcachedError',
    'MemcachedConnectionClosedError',
    '__version__',
]

logger = logging.getLogger(__name__)

class MemcachedError(Exception):
    pass

class MemcachedConnectionClosedError(MemcachedError):
    pass

DEFAULT_PORT = 11211
MAGIC_REQUEST = 0x80  ##   Request packet for this protocol version
MAGIC_RESPONSE = 0x81 ##   Response packet for this protocol version
MAX_KEY_SIZE =  0xFA  ##   Max key size in bytes

class F(object):
    """
    Compression/Serialization Flags
    """
    _pickle = 1 << 0
    _int = 1 << 1
    _long = 1 << 2
    _compressed = 1 << 3

class H(object):
    """
    Memcached Header Constants
    """
    _fmt = '!BBHBBHLLQ'
    _size = 24

class R(object):
    """
    Memcached Response Codes
    """
    _no_error = 0x0000
    _key_not_found = 0x0001
    _key_exists = 0x0002
    _value_too_large = 0x0003
    _invalid_arguments = 0x0004
    _items_not_stored = 0x0005
    _incr_decr_on_non_numeric_value = 0x0006
    _key_too_large = 0x0007 # custom addition

class M(object):
    """
    Memcached Protocol Constants
    """
    _get = 0x00
    _set = 0x01
    _add = 0x02
    _replace = 0x03
    _delete = 0x04
    _increment = 0x05
    _decrement = 0x06
    _quit = 0x07
    _flush = 0x08
    _getq = 0x09
    _noop = 0x0A
    _version = 0x0B
    _getk = 0x0C
    _getkq = 0x0D
    _append = 0x0E
    _prepend = 0x0F
    _stat = 0x10
    _setq = 0x11
    _addq = 0x12
    _replaceq = 0x13
    _deleteq = 0x14
    _incrementq = 0x15
    _decrementq = 0x16
    _quitq = 0x17
    _flushq = 0x18
    _appendq = 0x19
    _prependq = 0x1A


def _qnsv(opcode):
    """A quit/no-op/stat/version request packet"""
    return [
        struct.pack(H._fmt,
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            0,                  # key len
            0,                  # extlen
            0,                  # datatype
            0,                  # status
            0,                  # body len
            0,                  # opaque
            0,                  # cas
        )
    ]

def _f(opcode, expire):
    """A flush request packet"""
    return [
        struct.pack(H._fmt + 'I',
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            0,                  # key len
            4,                  # extlen
            0,                  # datatype
            0,                  # status
            4,                  # body len
            0,                  # opaque
            0,                  # cas
            expire,             # expire
        )
    ]

def _gd(opcode, key, opaque, cas):
    """A get* or delete* request packet"""
    return [
        struct.pack(H._fmt+'%ds' % len(key),
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            len(key),           # key len
            0,                  # extlen
            0,                  # datatype
            0,                  # status
            len(key),           # body len
            opaque,             # opaque
            cas,                # cas
            key,                # key
        )
    ]

def _s(opcode, key, val, opaque, expire, cas, flags):
    """A set*  or replace* packet"""
    return [
        struct.pack(H._fmt+'LL%ds%ds' % (len(key), len(val)),
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            len(key),           # key len
            8,                  # extlen
            0,                  # datatype
            0,                  # status
            len(key)+len(val)+8,# total body len
            opaque,             # opaque
            cas,                # cas
            flags,              # flags
            expire,             # expire
            key,                # key
            val,                # val
        )
    ]

def _ap(opcode, key, val, opaque, cas):
    """A prepend/append packet"""
    return [
        struct.pack(H._fmt+'%ds%ds' % (len(key), len(val)),
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            len(key),           # key len
            0,                  # extlen
            0,                  # datatype
            0,                  # status
            len(key)+len(val),  # total body len
            opaque,             # opaque
            cas,                # cas
            key,                # key
            val,                # val
        )
    ]

def _id(opcode, key, opaque, expire, cas, delta, initial):
    """An increment or decrement packet"""
    return [
        struct.pack(H._fmt+'qqL%ds' % (len(key),),
            MAGIC_REQUEST,      # magic
            opcode,             # get cmd
            len(key),           # key len
            20,                 # extlen
            0,                  # datatype
            0,                  # status
            len(key)+20,        # total body len
            opaque,             # opaque
            cas,                # cas
            delta,              # delta
            initial,            # initial
            expire,             # expire
            key,                # key
        )
    ]

# TODO: yada yada yada, if you send too much without receiving 
# the socket will eventually block. You can see this if you do
# a multiget with a very large (~100K) number of keys. I guess
# the fix is to use non-blocking sockets and do some reads when
# you hit EWOULDBLOCK. I'm waiting on that for now.
# in the mean time I am chunking getQ packets into blocks of 1K
# which prevents this from happening but slightly hurts performance
def socksend(sock, lst):
    sock.sendall(''.join(lst))

def sockrecv(sock, num_bytes):
    d = ''
    while len(d) < num_bytes:
        c = sock.recv(min(8192, num_bytes - len(d)))
        if not c:
            raise MemcachedConnectionClosedError('Connection closed')
        d += c
    return d

def sockresponse(sock):
    header = sockrecv(sock, H._size)
    magic, opcode, keylen, \
    extlen, datatype, status, \
    bodylen, opaque, cas = struct.unpack(H._fmt, header)

    if (magic != MAGIC_RESPONSE):
        logger.critical("MAGIC mismatch: client (%x) != server (%x); client "
                        "may not be compatible with this version of memcached "
                        "server!", MAGIC_RESPONSE, magic)
    if bodylen > 0:
        extra = sockrecv(sock, bodylen)
    else:
        extra = None

    return (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
            cas, extra)

def chunk(iterable, chunksize):
    it = iter(iterable)
    item = list(itertools.islice(it, chunksize))
    while item:
        yield item
        item = list(itertools.islice(it, chunksize))

class Client(object):
    def __init__(self, host_list, encode_fn=pickle.dumps,
                    decode_fn=pickle.loads, compress_fn=None,
                    decompress_fn=None, max_threads=None,
                    ch_replicas=100, default_encoding="utf-8"):
        """
        Create a new instance of the pymemc client.
        
        >>> c = Client('localhost:11211')
        >>> c.flush_all()
        True
        """
        if isinstance(host_list, str):
            host_list = [host_list]
        if not isinstance(host_list, list):
            raise Exception("host_list must be a list or single host str")

        self.encode_fn = encode_fn
        self.decode_fn = decode_fn
        self.compress_fn = compress_fn
        self.decompress_fn = decompress_fn
        self.threadpool = threadpool.ThreadPool(max_threads or len(host_list))
        self.hash = chash.ConsistentHash(replicas=ch_replicas)
        self.default_encoding = default_encoding
        # connect up sockets and add to chash
        for host_str in host_list:
            pool = connpool.SocketConnectionPool(self._parse_host(host_str))
            self.hash.add_node(pool)

    @contextlib.contextmanager
    def sock4key(self, key):
        r = self.hash.get_node(key)
        with connpool.pooled_connection(r) as sock:
            yield sock

    def _parse_host(self, host_str):
        if ":" in host_str:
            host, port = host_str.split(":")
        else:
            host = host_str
            port = DEFAULT_PORT
        return (host, int(port))
    
    def _encode_key(self, key):
        if self.default_encoding:
            key = key.encode(self.default_encoding)
        if len(key) > MAX_KEY_SIZE:
            raise MemcachedError("%d: Key Too Large" % (R._key_too_large,))
        return key
            
    def _encode(self, val):
        return self.encode_fn(val)

    def _decode(self, val):
        return self.decode_fn(val)

    def _compress(self, val):
        if self.compress_fn:
            return self.compress_fn(val)
        else:
            return val

    def _decompress(self, val):
        if self.decompress_fn:
            return self.decompress_fn(val)
        else:
            return val

    def _serialize(self, value):
        flags = 0
        if isinstance(value, str):
            pass
        elif isinstance(value, int):
            flags |= F._int
            value = str(value)
        elif isinstance(value, long):
            flags |= F._long
            value = str(value)
        else:
            flags |= F._pickle
            value = self._encode(value)

        if (flags & F._int) or (flags & F._long):
            return (flags, value)

        value = self._compress(value)
        flags |= F._compressed
        return (flags, value)

    def _deserialize(self, value, flags):
        if flags & F._compressed:
            value = self._decompress(value)
        if flags & F._int:
            return int(value)
        elif flags & F._long:
            return long(value)
        elif flags & F._pickle:
            return self._decode(value)
        return value

    def close(self):
        """
        Close the connections to all servers.
        
        >>> c = Client('localhost:11211')
        >>> c.close()
        """
        self.quit()
        for sock in self.hash.all_nodes():
            sock.close()

    def _g_helper(self, key, socket_fn, failure_test, unpack=True, return_cas=False):
        """
        helper for "get-like" commands
        """
        key = self._encode_key(key)
        with self.sock4key(key) as sock:
            socksend(sock, socket_fn(key))
            (_, _, _, _, _, status, bodylen, _, cas, extra) = sockresponse(sock)

        if status != R._no_error:
            if failure_test(status):
                return None
            else:
                raise MemcachedError("%d: %s" % (status, extra))

        if not unpack:
            return True

        flags, value = struct.unpack('!L%ds' % (bodylen - 4, ), extra)
        value = self._deserialize(value, flags)
        if return_cas:
            return value, cas
        else:
            return value

    def _s_helper(self, key, val, expire, socket_fn, failure_test, serialize=True):
        """
        helper for "set-like" commands
        """
        if serialize:
            flags, val = self._serialize(val)
        else:
            flags = 0
        
        key = self._encode_key(key)
        with self.sock4key(key) as sock:
            socksend(sock, socket_fn(key, val, expire, flags))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)
            if status != R._no_error:
                if failure_test(status):
                    return False
                else:
                    raise MemcachedError("%d: %s" % (status, extra))

        return True

    def _gmulti_helper(self, keys, hashkey, socket_fn, last_socket_fn):
        """
            helper for "multi_get-like" commands
        """
        def per_host_fn(sister_keys, response_map, hashkey=None):
            with self.sock4key(hashkey or sister_keys[0]) as sock:
                last_i = len(sister_keys)-1
                for i,key in enumerate(sister_keys):
                    if i == last_i:
                        socksend(sock, last_socket_fn(key, i))
                    else:
                        socksend(sock, socket_fn(key, i))
                while 1:
                    (_, _, _, _, _, status, bodylen, opaque, _, extra) = sockresponse(sock)
                    if status == R._no_error:
                        flags, value = struct.unpack('!L%ds' % (bodylen - 4, ), extra)
                        response_map[sister_keys[opaque]] = self._deserialize(value, flags)
                    if opaque == last_i: # last response?
                        break
        response_map = {}

        if hashkey:
            groups = [list(keys)]
        else:
            sock_keygroup_map = collections.defaultdict(list)
            for key in keys:
                key = self._encode_key(key)
                with self.sock4key(key) as sock:
                    sock_keygroup_map[id(sock)].append(key)
            groups = sock_keygroup_map.values()
        
        for group in groups:
            for small_group in chunk(group, 1000):
                self.threadpool.add_task(per_host_fn, small_group, response_map, hashkey=hashkey)
        self.threadpool.wait()
        return response_map

    def _smulti_helper(self, kvmap, expire, hashkey, socket_fn, last_socket_fn, failure_test):
        """
            helper for "multi_set-like" commands
        """
        def per_host_fn(sisters_map, failure_list, hashkey=None):
            items = sisters_map.items()
            last_i = len(items)-1
            with self.sock4key(hashkey or items[0][0]) as sock:
                for i,(key,val) in enumerate(items):
                    flags, val = self._serialize(val)
                    if i == last_i:
                        socksend(sock, last_socket_fn(key, val, i,
                                expire, flags))
                    else:
                        socksend(sock, socket_fn(key, val, i,
                                expire, flags))
                while 1:
                    (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque,
                            cas, extra) = sockresponse(sock)
                    if failure_test(status):
                        failure_list.append(items[opaque][0])
                    if opaque == last_i: # last item!
                        break
        failures = []
        
        if hashkey:
            # user is forcing everything to one shard
            groups = [kvmap]
        else:
            # group keys by the shard they hash to
            hash_groups = collections.defaultdict(dict)
            for key in kvmap.keys():
                key = self._encode_key(key)
                with self.sock4key(key) as sock:
                    hash_groups[id(sock)][key] = kvmap[key]
            groups = hash_groups.values()
                    
        for g in groups:
            self.threadpool.add_task(per_host_fn, g, failures, hashkey=hashkey)
        self.threadpool.wait()

        return failures

    def get(self, key, cas=False):
        """
        The get command returns the value for a single key.
        
        >>> c = Client('localhost:11211')
        >>> c.set('foo', 'bar')
        True
        >>> c.get('foo')
        'bar'
        """
        socket_fn = lambda key: _gd(M._get, key, 0, 0)
        failure_test = lambda status: status == R._key_not_found
        return self._g_helper(key, socket_fn, failure_test, return_cas=cas)

    def get_multi(self, keys, hashkey=None):
        """
        The get_multi command returns a dictionary mapping found keys to their
        values. Keys will be omitted if their value is not found.
        
        >>> c = Client('localhost:11211')
        >>> c.set_multi({'a':1, 'b':2})
        []
        >>> c.get_multi(['a', 'b'])
        {'a': 1, 'b': 2}
        """
        socket_fn = lambda key,opaque: _gd(M._getq, key, opaque, 0)
        last_socket_fn = lambda key,opaque: _gd(M._get, key, opaque, 0)
        return self._gmulti_helper(keys, hashkey, socket_fn, last_socket_fn)


    def set(self, key, val, expire=0, cas=0):
        """
        The set command sets a single key/val.
        
        >>> c = Client('localhost:11211')
        >>> c.set('bar', 'baz')
        True
        """
        socket_fn = lambda key,val,expire,flags: _s(M._set, key, val, 0, expire, cas, flags)
        failure_test = lambda status: status == R._items_not_stored or status == R._key_exists
        return self._s_helper(key, val, expire, socket_fn, failure_test)

    def set_multi(self, kvmap, expire=0, hashkey=None):
        """
        The set_multi command returns a list of keys that could not be set, or
        an empty list if all keys were successfully set.
        
        >>> c = Client('localhost:11211')
        >>> c.set_multi({'c':3, 'd':4})
        []
        """
        socket_fn = lambda key,value,opaque,expire,flags: _s(M._setq, key, value, opaque, expire, 0, flags)
        last_socket_fn = lambda key,value,opaque,expire,flags: _s(M._set, key, value, opaque, expire, 0, flags)
        failure_test = lambda status: status != R._no_error
        return self._smulti_helper(kvmap, expire, hashkey, socket_fn, last_socket_fn, failure_test)


    def add(self, key, val, expire=0, cas=0):
        """
        The add command sets a single key.
        Add MUST fail if the item already exists.
        
        >>> c = Client('localhost:11211')
        >>> c.set('already_added', 'val')
        True
        >>> c.add('newly_added', 'val')
        True
        >>> c.add('already_added', 'newval')
        False
        """
        socket_fn = lambda key,val,expire,flags: _s(M._add, key, val, 0, expire, cas, flags)
        failure_test = lambda status: status == R._key_exists or status == R._key_exists
        return self._s_helper(key, val, expire, socket_fn, failure_test)


    def add_multi(self, kvmap, expire=0, hashkey=None):
        """
        The add_multi command returns a list of keys that could not be added, or
        an empty list if all keys were successfully added.
        
        >>> c = Client('localhost:11211')
        >>> c.add_multi({'e':5, 'f':6})
        []
        >>> c.add_multi({'e':5, 'f':6})
        ['e', 'f']
        """
        socket_fn = lambda key,value,opaque,expire,flags: _s(M._addq, key, value, opaque, expire, 0, flags)
        last_socket_fn = lambda key,value,opaque,expire,flags: _s(M._add, key, value, opaque, expire, 0, flags)
        failure_test = lambda status: status != R._no_error
        return self._smulti_helper(kvmap, expire, hashkey, socket_fn, last_socket_fn, failure_test)


    def replace(self, key, val, expire=0, cas=0):
        """
        The replace command replaces a single key.
        Replace MUST fail if the item doesn't exist.
        
        >>> c = Client('localhost:11211')
        >>> c.set('replace_me', 'val')
        True
        >>> c.replace('missing', 'newval')
        False
        >>> c.replace('replace_me', 'newval')
        True
        """
        socket_fn = lambda key,val,expire,flags: _s(M._replace, key, val, 0, expire, cas, flags)
        failure_test = lambda status: status == R._key_not_found or status == R._key_exists
        return self._s_helper(key, val, expire, socket_fn, failure_test)

    def replace_multi(self, kvmap, expire=0, hashkey=None):
        """
        The replace_multi command returns a list of keys that could not be
        replaced, or an empty list if all keys were successfully replaced.

        >>> c = Client('localhost:11211')
        >>> c.set_multi({'g':7, 'h':8})
        []
        >>> c.replace_multi({'g':5, 'h':6})
        []
        >>> c.replace_multi({'x':5, 'y':6})
        ['y', 'x']
        """
        socket_fn = lambda key,value,opaque,expire,flags: _s(M._replaceq, key, value, opaque, expire, 0, flags)
        last_socket_fn = lambda key,value,opaque,expire,flags: _s(M._replace, key, value, opaque, expire, 0, flags)
        failure_test = lambda status: status == R._key_not_found or status == R._key_exists
        return self._smulti_helper(kvmap, expire, hashkey, socket_fn, last_socket_fn, failure_test)

    def delete(self, key, cas=0):
        """
        The delete command removes the value for a single key.
        True is returned on success, or False if the key was missing.
        
        >>> c = Client('localhost:11211')
        >>> c.set('delete_me', 'val')
        True
        >>> c.delete('missing')
        False
        >>> c.delete('delete_me')
        True
        """
        socket_fn = lambda key: _gd(M._delete, key, 0, cas)
        failure_test = lambda status: status == R._key_not_found or status == R._key_exists
        rval = self._g_helper(key, socket_fn, failure_test, unpack=False)
        return rval or False

    def delete_multi(self, keys, hashkey=None):
        """
        The delete_multi command removes the value for each key
        in a list of values. It returns the keys that could not
        be removed, or an empty list if all were successfully
        removed.

        >>> c = Client('localhost:11211')
        >>> c.set_multi({'i':9, 'j':10})
        []
        >>> c.delete_multi(['i', 'j'])
        []
        >>> c.delete_multi(['l', 'k'])
        ['l', 'k']
        """
        def per_host_delete(items, failure_list, hashkey=None):
            last_i = len(items)-1
            with self.sock4key(hashkey or items[0]) as sock:
                for i,key in enumerate(items):
                    if i == last_i:
                        socksend(sock, _gd(M._delete, key, i, 0))
                    else:
                        socksend(sock, _gd(M._deleteq, key, i, 0))
                while 1:
                    (_, _, _, _, _, status, _, opaque, _, _) = sockresponse(sock)
                    if status != R._no_error:
                        failure_list.append(items[opaque])
                    if opaque == last_i: # last item!
                        break
        failures = []
        if hashkey:
            # user is forcing everything to specific shard
            groups = [list(keys)]
        else:
            # group keys by the shard they live on
            groups = collections.defaultdict(list)
            for key in keys:
                key = self._encode_key(key)
                with self.sock4key(key) as sock:
                    groups[id(sock)].append(key)
            groups = groups.values()

        for g in groups:
            self.threadpool.add_task(per_host_delete, g, failures, hashkey=hashkey)
        self.threadpool.wait()

        return failures

    def incr(self, key, expire=0, delta=1, initial=0):
        """
        Increment key by the specified amount. If the key does
        not exist, create it with the value of initial.
        
        >>> c = Client('localhost:11211')
        >>> c.incr('incr', initial=1)
        1
        >>> c.incr('incr')
        2
        >>> c.incr('incr', delta=2)
        4
        """
        key = self._encode_key(key)
        with self.sock4key(key) as sock:
            socksend(sock, _id(M._increment, key, 0, expire, 0, delta, initial))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)

        if status != R._no_error:
            raise MemcachedError("%d: %s" % (status, extra))

        value, = struct.unpack('!Q', extra)
        return value

    def decr(self, key, expire=0, delta=1, initial=0):
        """
        Decrement key by the specified amount. If the key does
        not exist, create it with the value of initial.
        
        >>> c = Client('localhost:11211')
        >>> c.decr('decr', initial=10)
        10
        >>> c.decr('decr')
        9
        >>> c.decr('decr', delta=2)
        7
        """
        key = self._encode_key(key)
        with self.sock4key(key) as sock:
            socksend(sock, _id(M._decrement, key, 0, expire, 0, delta, initial))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)

        if status != R._no_error:
            raise MemcachedError("%d: %s" % (status, extra))

        value, = struct.unpack('!Q', extra)
        return value

    def append(self, key, val):
        """
        The append command will prepend the specified value to
        the requested key.
        
        >>> c = Client('localhost:11211')
        >>> c.set('app', 'aft')
        True
        >>> c.append('app', 'er')
        True
        >>> c.get('app')
        'after'
        """
        socket_fn = lambda key,val,expire,flags: _ap(M._append, key, val, 0, 0)
        failure_test = lambda status: status == R._items_not_stored
        return self._s_helper(key, val, 0, socket_fn, failure_test, serialize=False)

    def prepend(self, key, val):
        """
        The prepend command will prepend the specified value to
        the requested key.
        
        >>> c = Client('localhost:11211')
        >>> c.set('pre', 'fix')
        True
        >>> c.prepend('pre', 'pre')
        True
        >>> c.get('pre')
        'prefix'
        """
        socket_fn = lambda key,val,expire,flags: _ap(M._prepend, key, val, 0, 0)
        failure_test = lambda status: status == R._items_not_stored
        return self._s_helper(key, val, 0, socket_fn, failure_test, serialize=False)

    def quit(self):
        """
        The quit command closes the remote socket.

        >>> c = Client('localhost:11211')
        >>> c.quit()
        True
        """
        for sock in self.hash.all_nodes():
            socksend(sock, _qnsv(M._quit))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)
            if status != R._no_error:
                raise MemcachedError("%d: %s" % (status, extra))
        return True

    def flush_all(self, expire=0):
        """
        The flush command flushes all data the DB. Optionally this will happen
        after `expire` seconds.

        >>> c = Client('localhost:11211')
        >>> c.flush_all()
        True
        """
        for sock in self.hash.all_nodes():
            socksend(sock, _f(M._flush, expire))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)
            if status != R._no_error:
                raise MemcachedError("%d: %s" % (status, extra))
        return True

    def stats(self):
        """
        The stats command returns all statistics from the server.

        >>> c = Client('localhost:11211')
        >>> c.stats() #doctest: +ELLIPSIS
        {...
        """
        host_stats_map = {}
        def per_host_stats(sock, rmap):
            host_stats = {}
            socksend(sock, _qnsv(M._stat))
            while 1:
                (_, _, keylen, _, _, status, bodylen, _, _, extra) = sockresponse(sock)

                if status != R._no_error:
                    raise MemcachedError("%d: %s" % (status, extra))
                if keylen == 0: # last response?
                    host_key = "%s:%s" % sock.getpeername()
                    rmap[host_key] = host_stats
                    break
                else:
                    key, value = struct.unpack('!%ds%ds' % (keylen, (bodylen-keylen)), extra)
                    host_stats[key] = value

        for sock in self.hash.all_nodes():
            self.threadpool.add_task(per_host_stats, sock, host_stats_map)
        self.threadpool.wait()

        return host_stats_map

    def noop(self):
        """
        The noop command Flushes outstanding getq/getkq's and can be used
        as a keep-alive.

        >>> c = Client('localhost:11211')
        >>> c.noop()
        True
        """
        for sock in self.hash.all_nodes():
            socksend(sock, _qnsv(M._noop))
            (_, _, _, _, _, status, _, _, _, extra) = sockresponse(sock)
            if status != R._no_error:
                raise MemcachedError("%d: %s" % (status, extra))
        return True

    def version(self):
        """
        The version command returns the server's version string.

        >>> c = Client('localhost:11211')
        >>> c.version() #doctest: +ELLIPSIS
        {'...
        """
        host_version_map = {}
        def per_host_version(sock, rmap):
            socksend(sock, _qnsv(M._version))
            (_, _, keylen, _, _, status, bodylen, _, _, extra) = sockresponse(sock)

            if status != R._no_error:
                raise MemcachedError("%d: %s" % (status, extra))

            version_string = struct.unpack('!%ds' % ((bodylen-keylen), ), extra)[0]
            host_key = "%s:%s" % sock.getpeername()
            rmap[host_key] = version_string

        for sock in self.hash.all_nodes():
            self.threadpool.add_task(per_host_version, sock, host_version_map)
        self.threadpool.wait()

        return host_version_map
            
if __name__ == "__main__":
    # import doctest
    # doctest.testmod()
    c = Client("localhost:11211")
    c.set('keyhere', 'a'*10000000)
