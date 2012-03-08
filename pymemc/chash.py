import hashlib
import bisect

class ConsistentHash(object):
    def __init__(self, replicas=10):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        # many people use memcache with just a single node;
        # there is no need to waste time computing a hash
        # in this case
        self.single_node = True

    def hashkey(self, key):
        m = hashlib.md5(key)
        return long(m.hexdigest(), 16)

    def add_node(self, node):
        for i in xrange(self.replicas):
            key = self.hashkey("%s:%i" % (node,i))
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()
        if len(self.all_nodes()) > 1:
            self.single_node = False

    def get_node(self, key):
        if self.single_node:
            return self.ring[self.sorted_keys[0]]
        ckey = self.hashkey(key)
        if ckey > self.sorted_keys[-1]:
            return self.ring[self.sorted_keys[0]]
        index = bisect.bisect_left(self.sorted_keys, ckey)
        return self.ring[self.sorted_keys[index]]

    def all_nodes(self):
        return list(set(self.ring.values()))

