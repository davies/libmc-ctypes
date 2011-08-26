# -*- encoding:utf-8 -*-
import libmc
import unittest
import cPickle as pickle
import marshal
import time

TEST_SERVER = "localhost"

class BigObject(object):
    def __init__(self, letter='1', size=2000000):
        self.object = letter * size

    def __eq__(self, other):
        return self.object == other.object

class NoPickle(object):
    def __getattr__(self, name):
        pass

class TestCmemcached(unittest.TestCase):

    def setUp(self):
        self.mc=libmc.Client([TEST_SERVER], comp_threshold=1024)

    def test_set_get(self):
        self.mc.set("key", "value")
        self.assertEqual(self.mc.get("key") , "value")

        self.mc.set("key_int", 1)
        self.assertEqual(self.mc.get("key_int") , 1)

        self.mc.set("key_long", 1234567890L)
        self.assertEqual(self.mc.get("key_long") , 1234567890L)

        self.mc.set("key_object", BigObject())
        self.assertEqual(self.mc.get("key_object"),BigObject())

        big_object=BigObject('x', 1000001)
        self.mc.set("key_big_object", big_object)
        self.assertEqual(self.mc.get("key_big_object"),big_object)

    def test_chinese_set_get(self):
        key='豆瓣'
        value='在炎热的夏天我们无法停止上豆瓣'
        self.assertEqual(self.mc.set(key, value),1)

        self.assertEqual(self.mc.get(key) , value)

    def test_special_key(self):
        key='keke a kid'
        value=1024
        self.assertEqual(self.mc.set(key,value),0)
        self.assertEqual(self.mc.get(key),None)
        key='u:keke a kid'
        self.assertEqual(self.mc.set(key,value),0)
        self.assertEqual(self.mc.get(key),None)

    def test_empty_string(self):
        key='ttt'
        value=''
        self.assertEqual(self.mc.set(key,value), True)
        self.assertEqual(self.mc.get(key), '')

    def test_add(self):
        key = 'test_add'
        self.mc.delete(key)
        self.assertEqual(self.mc.add(key, 'tt'), 1)
        self.assertEqual(self.mc.get(key), 'tt')
        self.assertEqual(self.mc.add(key, 'tt'), 0)
        self.mc.delete(key+'2')
        self.assertEqual(self.mc.add(key+'2', range(10)), 1)

    def test_replace(self):
        key = 'test_replace'
        self.mc.delete(key)
        self.assertEqual(self.mc.replace(key, ''), 0)
        self.assertEqual(self.mc.set(key, 'b'), 1)
        self.assertEqual(self.mc.replace(key, 'a'), 1)
        self.assertEqual(self.mc.get(key), 'a')

    def test_append(self):
        key="test_append"
        value="append\n"
        self.mc.delete(key)
        self.assertEqual(self.mc.append(key, value), 0)
        self.mc.set(key, "")
        self.assertEqual(self.mc.append(key, value), 1)
        self.assertEqual(self.mc.append(key, value), 1)
        self.assertEqual(self.mc.prepend(key, 'before\n'), 1)
        self.assertEqual(self.mc.get(key), 'before\n' + value * 2)

    def test_append_multi(self):
        N = 10
        K = "test_append_multi_%d"
        data = "after\n"
        for i in range(N):
            self.assertEqual(self.mc.set(K%i, "before\n"), 1)
        keys = [K%i for i in range(N)]
        self.assertEqual(self.mc.append_multi(keys, data), 1)
        self.assertEqual(self.mc.get_multi(keys), dict(zip(keys, ["before\n"+data] * N)))
        # prepend
        self.assertEqual(self.mc.prepend_multi(keys, data), 1)
        self.assertEqual(self.mc.get_multi(keys), dict(zip(keys, [data+"before\n"+data] * N)))
        # delete
        self.assertEqual(self.mc.delete_multi(keys), 1)
        self.assertEqual(self.mc.get_multi(keys), {})

    def test_append_multi_performance(self):
        N = 50000
        K = "test_append_multi_%d"
        data = "after\n"
        keys = [K%i for i in range(N)]
        t = time.time()
        self.mc.append_multi(keys, data)
        t = time.time() - t
        assert t < 1, 'should append 5k key in 1 secs %f' % t

    def test_set_multi(self):
        values = dict(('key%s'%k,('value%s'%k)*100000*k) for k in range(10))
        values.update({' ':''})
        self.assertEqual(self.mc.set_multi(values), 1)
        
        del values[' ']
        for k in values:
            self.assertEqual(self.mc.get(k), values[k])
        
        mc=libmc.Client(["localhost:11999"], comp_threshold=1024)
        self.assertEqual(mc.set_multi(values), 0)

    def test_append_large(self):
        k = 'test_append_large'
        self.mc.set(k, 'a' * 2048)
        self.mc.append(k, 'bbbb')
        assert 'bbbb' not in self.mc.get(k)
        self.mc.set(k, 'a' * 2048, compress=False)
        self.mc.append(k, 'bbbb')
        assert 'bbbb' in self.mc.get(k)

    def test_incr(self):
        key="Not_Exist"
        self.assertEqual(self.mc.incr(key), None)
        #key="incr:key1"
        #self.mc.set(key, "not_numerical")
        #self.assertEqual(self.mc.incr(key), 0)
        key="incr:key2"
        self.mc.set(key, 2007)
        self.assertEqual(self.mc.incr(key), 2008)
        
    def test_decr(self):
        key="Not_Exist"
        self.assertEqual(self.mc.decr(key),None)
        #key="decr:key1"
        #self.mc.set(key, "not_numerical")
        #self.assertEqual(self.mc.decr(key),0)
        key="decr:key2"
        self.mc.set(key, 2009)
        self.assertEqual(self.mc.decr(key),2008)

    def test_get_multi(self):
        keys=["hello1", "hello2", "hello3"]
        values=["vhello1", "vhello2", "vhello3"]
        for x in xrange(3):
            self.mc.set(keys[x], values[x])
            self.assertEqual(self.mc.get(keys[x]) , values[x])
        self.assertEqual(self.mc.get_multi(keys), dict(zip(keys, values)))

    def test_get_multi_big(self):
        keys=["hello1", "hello2", "hello3"]
        values=[BigObject(str(i), 1000001) for i in xrange(3)]
        for x in xrange(3):
            self.mc.set(keys[x], values[x])
            self.assertEqual(self.mc.get(keys[x]) , values[x])
        result=self.mc.get_multi(keys)
        for x in xrange(3):
            self.assertEqual(result[keys[x]] , values[x])

    def test_get_multi_with_empty_string(self):
        keys=["hello1", "hello2", "hello3"]
        for k in keys:
            self.mc.set(k, '')
        self.assertEqual(self.mc.get_multi(keys), dict(zip(keys,[""]*3)))

    def testBool(self):
        self.mc.set("bool", True)
        value = self.mc.get("bool")
        self.assertEqual(value, True)
        self.mc.set("bool_", False)
        value = self.mc.get("bool_")
        self.assertEqual(value, False)

    def testEmptyString(self):
        self.mc.set("str", '')
        value = self.mc.get("str")
        self.assertEqual(value, '')

    def testGetHost(self):
        self.mc.set("str", '')
        host = self.mc.get_host_by_key("str")
        self.assertEqual(host, TEST_SERVER)

    def test_get_list(self):
        self.mc.set("a", 'a')
        v = self.mc.get_list(['a','b'])
        self.assertEqual(v, ['a',None])

    def test_marshal(self):
        v = [{2:{"a": 337}}]
        self.mc.set("a", v)
        self.assertEqual(self.mc.get("a"), v)
        raw, flags = self.mc.get_raw("a")
        self.assertEqual(raw, marshal.dumps(v, 2))

    def test_pickle(self):
        v = [{"v": BigObject('a', 10)}]
        self.mc.set("a", v)
        self.assertEqual(self.mc.get("a"), v)
        raw, flags = self.mc.get_raw("a")
        self.assertEqual(raw, pickle.dumps(v, -1))
    
    def test_no_pickle(self):
        v = NoPickle()
        self.assertEqual(self.mc.set("nopickle", v), False)
        self.assertEqual(self.mc.get("nopickle"), None)

    def test_big_list(self):
        v = range(1024*1024)
        self.assertEqual(self.mc.set('big_list', v), 1)
        self.assertEqual(self.mc.get('big_list'), v)

    def test_last_error(self):
        self.assertEqual(self.mc.set('testkey', 'hh'), True)
        self.assertEqual(self.mc.get('testkey'), 'hh')
        self.assertEqual(self.mc.get_last_error(), 0)
        
        self.mc=libmc.Client(["localhost:11999"], comp_threshold=1024)
        self.assertEqual(self.mc.set('testkey', 'hh'), False)
        self.assertEqual(self.mc.get('testkey'), None)
        self.assertNotEqual(self.mc.get_last_error(), 1)

    def test_stats(self):
        s = self.mc.stats()
        self.assertEqual(TEST_SERVER in s, True)
        st = s[TEST_SERVER]
        st_keys = sorted([
          "pid",
          "uptime",
          "time",
          "version",
          "pointer_size",
          "rusage_user",
          "rusage_system",
          "curr_items",
          "total_items",
          "bytes",
          "curr_connections",
          "total_connections",
          "connection_structures",
          "cmd_get",
          "cmd_set",
          "get_hits",
          "get_misses",
          "evictions",
          "bytes_read",
          "bytes_written",
          "limit_maxbytes",
          "threads",
        ])
        self.assertEqual(sorted(st.keys()), st_keys)
        
        mc=libmc.Client(["localhost:11999", TEST_SERVER])
        s = mc.stats()
        self.assertEqual(len(s), 2)

    #def test_gets_multi(self):
    #    keys=["hello1", "hello2", "hello3"]
    #    values=["vhello1", "vhello2", "vhello3"]
    #    for x in xrange(3):
    #        self.mc.set(keys[x], values[x])
    #        self.assertEqual(self.mc.get(keys[x]) , values[x])
    #    result=self.mc.gets_multi(keys)
    #    for x in xrange(3):
    #        #print result[keys[x]][0],result[keys[x]][1]
    #        self.assertEqual(result[keys[x]][0] , values[x])

    #def test_cas(self):
    #    keys=["hello1", "hello2", "hello3"]
    #    values=["vhello1", "vhello2", "vhello3"]
    #    for x in xrange(3):
    #        self.mc.set(keys[x], values[x])
    #        self.assertEqual(self.mc.get(keys[x]) , values[x])
    #    result=self.mc.gets_multi(keys)
    #    for x in xrange(3):
    #        self.assertEqual(result[keys[x]][0] , values[x])
    #        self.assertEqual(self.mc.cas(keys[x],'cas',cas=result[keys[x]][1]) , 1)
    #        self.assertEqual(self.mc.cas(keys[x],'cas2',cas=result[keys[x]][1]) , 0)
    #        self.assertEqual(self.mc.get(keys[x]) , 'cas')


class TestBinaryCmemcached(TestCmemcached):

    def setUp(self):
        self.mc=libmc.Client([TEST_SERVER], comp_threshold=1024)
        self.mc.set_behavior(libmc.BEHAVIOR_BINARY_PROTOCOL, 1)

    def test_append_multi_performance(self):
        "binary is slow, bug ?"

    def test_stats(self):
        "not yet support"

if __name__ == '__main__':
    unittest.main()
