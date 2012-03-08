import os
import sys
sys.path.append("..")
import unittest
import socket
import random
import base64
import pymemc

# these are not really unit tests but I'm using the unittest framework.
# to run them you need a local memcache server(s) running on the host/ports
# specified in HOST_STRINGS.
# these tests will flush the cache, so beware!

HOST_STRINGS = ['localhost:11211', 'localhost:11212', 'localhost:11213', 'localhost:11214']
HOST_STRINGS = HOST_STRINGS[:1] # comment this out to run across multiple servers

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.client = pymemc.Client(HOST_STRINGS)
        self.client.flush_all()

    def tearDown(self):
        try:
            self.client.flush_all()
            self.client.close()
        except socket.error:
            # can if we close our connection before here
            # like when testing "quit"
            pass

    def random_str(self, length=5):
         return base64.urlsafe_b64encode(os.urandom(length))

    def get_sample_data(self, length=100):
        keys = (self.random_str(5) for i in xrange(length))
        vals = (self.random_str(100) for i in xrange(length))
        sample_data = dict(zip(keys, vals))
        return sample_data

class TestQuitNoop(BaseTest):
    def testQuit(self):
        """test quit command"""
        assert self.client.quit() == True
        self.assertRaises(pymemc.MemcachedError, self.client.quit)

    def testNoop(self):
        """test that noop is a... no-op?"""
        for i in xrange(100):
            assert self.client.noop() == True

class TestGetSetDelete(BaseTest):
    def testGetSet(self):
        """test simple gets and sets"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            assert self.client.set(key, val) == True

        for key in sample_data.iterkeys():
            assert sample_data[key] == self.client.get(key)

    def testGetSetMissingValues(self):
        """test simple gets and sets with missing values"""
        sample_data = self.get_sample_data()

        for i, (key, val) in enumerate(sample_data.iteritems()):
            if (i % 2) == 0:
                assert self.client.set(key, val) == True

        for i, (key, val) in enumerate(sample_data.iteritems()):
            if (i % 2) == 0:
                assert sample_data[key] == self.client.get(key)
            else:
                assert self.client.get(key) == None

    def testSetOversizeKey(self):
        """test oversize keys"""
        # max default key size = 255 bytes
        oversized_key = self.random_str(length=1000)
        normal_val = self.random_str(length=1000)

        self.assertRaises(pymemc.MemcachedError, self.client.set,
            oversized_key, normal_val)

    def testSetOversizeValue(self):
        """test oversize values"""
        # max default val size = 1MB
        normal_key = self.random_str(length=100)
        oversized_val = self.random_str(length=10000000)

        self.assertRaises(pymemc.MemcachedError, self.client.set,
            normal_key, oversized_val)

    def testDeleteSet(self):
        """test setting, getting, deleting"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            self.client.set(key,val)

        for key,val in sample_data.iteritems():
            assert self.client.get(key) == val

        for key in sample_data.iterkeys():
            assert self.client.delete(key) == True

        for key in sample_data.iterkeys():
            assert self.client.delete(key) == False

        for key,val in sample_data.iteritems():
            assert self.client.get(key) == None

class TestAddReplace(BaseTest):
    def testAddGet(self):
        """test basic adds"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            assert self.client.add(key, val) == True

        for key in sample_data.iterkeys():
            assert sample_data[key] == self.client.get(key)

    def testAddGetExisting(self):
        """test adds with existing keys"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            assert self.client.set(key, val) == True

        for key, val in sample_data.iteritems():
            reversed_val = val[::-1]
            assert self.client.add(key, reversed_val) == False

        for key in sample_data.iterkeys():
            assert sample_data[key] == self.client.get(key)

    def testReplaceGet(self):
        """test basic replaces"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            assert self.client.set(key, val) == True

        for key, val in sample_data.iteritems():
            reversed_val = val[::-1]
            assert self.client.replace(key, reversed_val) == True

        for key, val in sample_data.iteritems():
            reversed_val = val[::-1]
            assert reversed_val == self.client.get(key)

    def testReplaceGetMissing(self):
        """test replace with missing keys"""
        sample_data = self.get_sample_data(length=100)

        for key, val in sample_data.iteritems():
            assert self.client.replace(key, val) == False

class TestIncrementDecrement(BaseTest):
    def testIncrement(self):
        """test basic increment"""
        int_sample_data = dict(zip(map(str, xrange(100)), xrange(100)))
        random_delta = random.randint(1,10)
        for key,intval in int_sample_data.iteritems():
            assert self.client.incr(key, initial=intval) == intval
            assert self.client.incr(key) == intval+1
            assert self.client.incr(key, delta=random_delta) == intval+1+random_delta

    def testDecrement(self):
        int_sample_data = dict(zip(map(str, xrange(100,200)), xrange(100,200)))
        random_delta = random.randint(1,10)
        for key,intval in int_sample_data.iteritems():
            assert self.client.set(key, intval) == True
            assert self.client.decr(key) == intval-1
            assert self.client.decr(key, delta=random_delta) == intval-1-random_delta

class TestMultiGetSetDelete(BaseTest):
    def testSetMulti(self):
        """test simple multisets"""
        sample_data = self.get_sample_data(length=100)

        assert self.client.set_multi(sample_data) == []

        for key in sample_data.iterkeys():
            assert sample_data[key] == self.client.get(key)

    def testGetMulti(self):
        """test simple multigets"""
        sample_data = self.get_sample_data(length=100)
        assert self.client.set_multi(sample_data) == []

        return_data = self.client.get_multi(sample_data.iterkeys())

        for key in sample_data.iterkeys():
            assert sample_data[key] == return_data[key]

    def testGetSetMultiFuckTon(self):
        """test multigets and multisets with many values"""
        sample_data = self.get_sample_data(length=50000)
        assert self.client.set_multi(sample_data) == []
        rdata = self.client.get_multi(sample_data.iterkeys())
        for key in sample_data.iterkeys():
            assert sample_data[key] == self.client.get(key) == rdata[key]

    def testGetMultiMissing(self):
        """test multigets with missing values"""
        sample_data = self.get_sample_data(length=100)
        assert self.client.get_multi(sample_data.iterkeys()) == {}

    def testDeleteMulti(self):
        """test simple multideletes"""
        sample_data = self.get_sample_data(length=100)
        assert self.client.set_multi(sample_data) == []

        return_data = self.client.get_multi(sample_data.iterkeys())

        for key in sample_data.iterkeys():
            assert sample_data[key] == return_data[key]

        assert self.client.delete_multi(sample_data.iterkeys()) == []

    def testMultiHashKey(self):
        """test hashkey multi-ops"""
        sample_data = self.get_sample_data(length=100)
        assert self.client.set_multi(sample_data, hashkey='test') == []

        return_data = self.client.get_multi(sample_data.iterkeys(), hashkey='test')

        for key in sample_data.iterkeys():
            assert sample_data[key] == return_data[key]

        assert self.client.delete_multi(sample_data.iterkeys(), hashkey='test') == []

    def testDeleteMultiMissing(self):
        """test multideletes with missing values"""
        sample_data = self.get_sample_data(length=100)
        assert set(self.client.delete_multi(sample_data.iterkeys())) == set(sample_data.iterkeys())

class TestMultiAddReplace(BaseTest):
    def testReplaceMulti(self):
        """test simple multireplaces"""
        sample_data = self.get_sample_data(length=100)

        assert self.client.set_multi(sample_data) == []

        rmap = dict(sample_data)
        for key,val in rmap.iteritems():
            rmap[key] = val[::-1]

        assert self.client.replace_multi(rmap) == []

        for key in rmap.iterkeys():
            assert rmap[key] == self.client.get(key)

    def testReplaceMultiMissing(self):
        """test multireplaces with missing values"""
        sample_data = self.get_sample_data(length=100)

        assert set(self.client.replace_multi(sample_data)) == set(sample_data.iterkeys())

    def testAddMulti(self):
        """test simple multiadds"""
        sample_data = self.get_sample_data(length=100)

        assert self.client.add_multi(sample_data) == []

        for key,val in sample_data.iteritems():
            assert self.client.get(key) == val

    def testAddMultiExisting(self):
        """test multiadds with existing keys"""
        sample_data = self.get_sample_data(length=100)

        assert self.client.add_multi(sample_data) == []
        assert set(self.client.add_multi(sample_data)) == set(sample_data.iterkeys())


# test complex objects
# test set_many with 10000 things
# test get_many with 10000 things
# test all CAS methods
# test hashkeys

def main():
    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')
    unittest.main()

if __name__ == "__main__":
    main()