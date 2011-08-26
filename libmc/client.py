import sys, ctypes
import marshal, cPickle, zlib
from zlib import compress, decompress, error as zlib_error
from libmemcached import c

MEMCACHED_MAX_KEY = 200

MEMCACHED_SUCCESS = 0
MEMCACHED_FAILURE = 1
MEMCACHED_HOST_LOOKUP_FAILURE = 2
MEMCACHED_CONNECTION_FAILURE = 3
MEMCACHED_CONNECTION_BIND_FAILURE = 4
MEMCACHED_WRITE_FAILURE = 5
MEMCACHED_READ_FAILURE = 6
MEMCACHED_UNKNOWN_READ_FAILURE = 7
MEMCACHED_PROTOCOL_ERROR = 8
MEMCACHED_CLIENT_ERROR = 9
MEMCACHED_SERVER_ERROR = 10
MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE = 11
MEMCACHED_DATA_EXISTS = 12
MEMCACHED_DATA_DOES_NOT_EXIST = 13
MEMCACHED_NOTSTORED = 14
MEMCACHED_STORED = 15
MEMCACHED_NOTFOUND = 16
MEMCACHED_MEMORY_ALLOCATION_FAILURE = 17
MEMCACHED_PARTIAL_READ = 18
MEMCACHED_SOME_ERRORS = 19
MEMCACHED_NO_SERVERS = 20
MEMCACHED_END = 21
MEMCACHED_DELETED = 22
MEMCACHED_VALUE = 23
MEMCACHED_STAT = 24
MEMCACHED_ERRNO = 25
MEMCACHED_FAIL_UNIX_SOCKET = 26
MEMCACHED_NOT_SUPPORTED = 27
MEMCACHED_NO_KEY_PROVIDED = 28
MEMCACHED_FETCH_NOTFINISHED = 29
MEMCACHED_TIMEOUT = 30
MEMCACHED_BUFFERED = 31
MEMCACHED_BAD_KEY_PROVIDED = 32
MEMCACHED_INVALID_HOST_PROTOCOL = 33
MEMCACHED_SERVER_MARKED_DEAD = 34
MEMCACHED_UNKNOWN_STAT_KEY = 35
MEMCACHED_E2BIG = 36
MEMCACHED_INVALID_ARGUMENTS = 37
MEMCACHED_KEY_TOO_BIG = 38
MEMCACHED_AUTH_PROBLEM = 39
MEMCACHED_AUTH_FAILURE = 40

BEHAVIOR_NO_BLOCK               = 0 
BEHAVIOR_TCP_NODELAY            = 1
BEHAVIOR_HASH                   = 2
BEHAVIOR_KETAMA                 = 3
BEHAVIOR_SOCKET_SEND_SIZE       = 4
BEHAVIOR_SOCKET_RECV_SIZE       = 5
BEHAVIOR_CACHE_LOOKUPS          = 6
BEHAVIOR_SUPPORT_CAS            = 7
BEHAVIOR_POLL_TIMEOUT           = 8
BEHAVIOR_DISTRIBUTION           = 9
BEHAVIOR_BUFFER_REQUESTS        = 10
BEHAVIOR_USER_DATA              = 11
BEHAVIOR_SORT_HOSTS             = 12
BEHAVIOR_VERIFY_KEY             = 13
BEHAVIOR_CONNECT_TIMEOUT        = 14
BEHAVIOR_RETRY_TIMEOUT          = 15
BEHAVIOR_KETAMA_WEIGHTED        = 16
BEHAVIOR_KETAMA_HASH            = 17
BEHAVIOR_BINARY_PROTOCOL        = 18
BEHAVIOR_SND_TIMEOUT            = 19
BEHAVIOR_RCV_TIMEOUT            = 20
BEHAVIOR_SERVER_FAILURE_LIMIT   = 21
BEHAVIOR_IO_MSG_WATERMARK        = 22 
BEHAVIOR_IO_BYTES_WATERMARK      = 23
BEHAVIOR_IO_KEY_PREFETCH         = 24
BEHAVIOR_HASH_WITH_PREFIX_KEY    = 25
BEHAVIOR_NOREPLY                 = 26
BEHAVIOR_USE_UDP                 = 27
BEHAVIOR_AUTO_EJECT_HOSTS        = 28
BEHAVIOR_NUMBER_OF_REPLICAS      = 29
BEHAVIOR_RANDOMIZE_REPLICA_READ  = 30
BEHAVIOR_CORK                    = 31
BEHAVIOR_TCP_KEEPALIVE           = 32
BEHAVIOR_TCP_KEEPIDLE            = 33

_FLAG_PICKLE = 1<<0
_FLAG_INTEGER = 1<<1
_FLAG_LONG = 1<<2
_FLAG_BOOL = 1<<3
_FLAG_COMPRESS = 1<<4
_FLAG_MARSHAL = 1<<5
_FLAG_CHUNKED = 1<<12

CHUNK_SIZE = 1000000

VERSION="0.40"

def prepare(val, comp_threshold):
    flag = 0

    if isinstance(val, basestring):
        pass
    elif isinstance(val, (bool)):
        flag = _FLAG_BOOL
        val = str(int(val))
    elif isinstance(val, (int,long)):
        flag = _FLAG_INTEGER
        val = str(val)
    else:
        try:
            val = marshal.dumps(val, 2)
            flag = _FLAG_MARSHAL
        except ValueError, e:
            try:
                val = cPickle.dumps(val, -1)
                flag = _FLAG_PICKLE
            except Exception, e:
                val = None

    if comp_threshold > 0 and val and len(val) > comp_threshold:
        val = zlib.compress(val)
        flag |= _FLAG_COMPRESS

    # split

    return val, flag

def restore(val, flag):
    if val is None:
        return val

    # restore splited

    if flag & _FLAG_COMPRESS:
        try:
            val = decompress(val)
        except zlib_error:
            return None
        flag &= ~_FLAG_COMPRESS

    if flag == 0:
        pass
    elif flag & _FLAG_BOOL:
        val = bool(int(val))
    elif flag & _FLAG_INTEGER:
        val = int(val)
    elif flag & _FLAG_LONG:
        val = long(val)
    elif flag & _FLAG_MARSHAL:
        try:
            val = marshal.loads(val)
        except Exception, e:
            val = None
    elif flag & _FLAG_PICKLE:
        try:
            val = cPickle.loads(val)
        except Exception, e:
            val = None
    return val

from binascii import hexlify
import re

def dump(obj):
    # helper function to dump memory contents in hex, with a hyphen
    # between the bytes.
    h = hexlify(buffer(obj))
    return re.sub(r"(..)", r"\1-", h)[:-1]

class Client(object):
    "Client for memcached"

    def __init__(self, servers, do_split=1, comp_threshold=0, *a, **kw):
        self.mc = c.memcached_create(None)
        self.servers = []
        self.do_split = do_split
        self.comp_threshold = comp_threshold
        
        self.add_server(servers)
        self.set_behavior(BEHAVIOR_NO_BLOCK, 1) # nonblock
        self.set_behavior(BEHAVIOR_TCP_NODELAY, 1) # nonblock
        self.set_behavior(BEHAVIOR_TCP_KEEPALIVE, 1)
        self.set_behavior(BEHAVIOR_CACHE_LOOKUPS, 1)
        self.set_behavior(BEHAVIOR_KETAMA, 1)
        #self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 0) # no request buffer

    def __dealloc__(self):
        c.memcached_free(self.mc)

    def add_server(self, servers):
        sl = c.memcached_servers_parse(",".join(servers))
        ret = c.memcached_server_push(self.mc, sl)
        c.memcached_server_list_free(sl)
        self.servers += servers       
 
    def get_host_by_key(self, key):
        hash = c.memcached_generate_hash(self.mc, key, len(key))
        return self.servers[hash]

    def get_last_error(self):
        return self.last_error

    def set_behavior(self, flag, behavior):
        return c.memcached_behavior_set(self.mc, flag, behavior)

    def get_behavior(self, flag):
        return c.memcached_behavior_get(self.mc, flag)

    def check_key(self, key):
        if not key:
            return False
        if len(key) >= MEMCACHED_MAX_KEY:
            return False
        for c in key:
            if 0<= ord(c) <= 32:
                return False
        return True

    def set_raw(self, key, val, expire, flag):
        if not self.check_key(key):
            return False

        if len(val) > CHUNK_SIZE and self.do_split:
            i, bytes = 0, len(val)
            if bytes > CHUNK_SIZE * 10:
                return False
            while val:
                chunk_key = "~%s/%d" % (key, i)
                ret = c.memcached_set(self.mc, chunk_key, len(chunk_key), 
                    val[:CHUNK_SIZE], min(len(val),CHUNK_SIZE), expire, 0)
                if ret not in (MEMCACHED_SUCCESS, MEMCACHED_NOTSTORED, MEMCACHED_STORED):
                    return False
                i += 1
                val = val[CHUNK_SIZE:]
            body = "%d" % i
            ret = c.memcached_set(self.mc, key, len(key), body, len(body), expire, flag | _FLAG_CHUNKED)

        else:   
            ret = c.memcached_set(self.mc, key, len(key), val, len(val), expire, flag)

        return ret in (MEMCACHED_SUCCESS, MEMCACHED_NOTSTORED, MEMCACHED_STORED)

    def set(self, key, val, expire=0, compress=True):
        comp = compress and self.comp_threshold or 0
        val, flag = prepare(val, comp)
        if val is not None:
            return self.set_raw(key, val, expire, flag)
        return False

    def set_multi_raw(self, values, expire):
        self.set_behavior(BEHAVIOR_NOREPLY, 1)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 1)
        for key, (val, flag) in values.iteritems():
            if not self.check_key(key):
                continue
            self.set_raw(key, val, expire, flag)
        retval = c.memcached_flush_buffers(self.mc)
        self.set_behavior(BEHAVIOR_NOREPLY, 0)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 0)
        return retval == MEMCACHED_SUCCESS        

    def set_multi(self, values, time=0, compress=True):
        comp = compress and self.comp_threshold or 0
        raw_values = dict((k, prepare(v, comp)) for k,v in values.iteritems())
        return self.set_multi_raw(raw_values, time)

    def _get_raw_split(self, key, count, flag):
        vals = []
        for i in range(count):
            chunk_key = "~%s/%d" % (key, i)
            v, f = self.get_raw(chunk_key)
            if not v: return None, 0
            vals.append(v)
        return "".join(vals), flag & (~_FLAG_CHUNKED)

    def get_raw(self, key):
        if not self.check_key(key):
            return None, 0
        self.last_error = 0
        bytes, flag, rc = ctypes.c_size_t(0), ctypes.c_uint32(0), ctypes.c_int(0)
        c_val = c.memcached_get(self.mc, key, len(key), ctypes.byref(bytes), 
                ctypes.byref(flag), ctypes.byref(rc))
        if not c_val:
            return None, 0
        val, flag = ctypes.string_at(c_val, bytes.value), flag.value

        if flag & _FLAG_CHUNKED:
            val, flag = self._get_raw_split(key, int(val), flag)

        return val, flag

    def get(self, key):
        val, flag = self.get_raw(key)
        return restore(val, flag)

    def get_multi_raw(self, keys):
        keys = [key for key in keys if self.check_key(key)]
        n = len(keys)
        ckeys = (ctypes.c_char_p * n)(*keys)
        lens = (ctypes.c_size_t * n)(*[len(k) for k in keys])
        c.memcached_mget(self.mc, ckeys, lens, n)    

        result = {}
        chunks_record = []
        while True:
            key = ctypes.create_string_buffer(200)
            klen, vlen, flag, rc = ctypes.c_size_t(0), ctypes.c_size_t(0), ctypes.c_uint32(0), ctypes.c_int(0)
            val = c.memcached_fetch(self.mc, key, ctypes.byref(klen), 
                ctypes.byref(vlen), ctypes.byref(flag), ctypes.byref(rc))
            if not val:
                break
            key = key.value
            val = ctypes.string_at(val, vlen.value)
            flag = flag.value
            if flag & _FLAG_CHUNKED:
                chunks_record.append((key, int(val), flag))
            else:
                result[key] = (val, flag)
        
        for key, count, flag in chunks_record:
            val, flag = self._get_raw_split(key, count, flag)
            if val: 
                result[key] = (val, flag)
        
        return result

    def get_multi(self, keys):
        result = self.get_multi_raw(keys)
        return dict((k, restore(v, flag))
                    for k, (v, flag) in result.iteritems())

    def get_list(self, keys):
        result = self.get_multi(keys)
        return [result.get(key) for key in keys]

    def delete(self, key, delay=0):
        if not self.check_key(key):
            return 0
        ret = c.memcached_delete(self.mc, key, len(key), delay)
        return ret in (MEMCACHED_SUCCESS, MEMCACHED_NOTFOUND)

    def delete_multi(self, keys, delay=0):
        self.set_behavior(BEHAVIOR_NOREPLY, 1)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 1)
        for key in keys:
            if self.check_key(key):
                c.memcached_delete(self.mc, key, len(key), delay)
        retval = c.memcached_flush_buffers(self.mc)
        self.set_behavior(BEHAVIOR_NOREPLY, 0)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 0)

        return retval == MEMCACHED_SUCCESS        

    def _store(self, cmd, key, val, expire=0, cas=0, expected=(MEMCACHED_SUCCESS,)):
        if not self.check_key(key):
            return False
        if cmd in  ('append', 'prepend') and type(val) != type(''):
            return False

        val, flag = prepare(val, self.comp_threshold)
        if cmd == 'add':
            ret = c.memcached_add(self.mc, key, len(key), val, len(val), expire, flag)
        elif cmd == 'replace':
            ret = c.memcached_replace(self.mc, key, len(key), val, len(val), expire, flag)
        elif cmd == 'cas':
            ret = c.memcached_cas(self.mc, key, len(key), val, len(val), expire, flag, cas)
        elif cmd == 'append':
            ret = c.memcached_append(self.mc, key, len(key), val, len(val), expire, flag)
        elif cmd == 'prepend':
            ret = c.memcached_prepend(self.mc, key, len(key), val, len(val), expire, flag)
        else:
            raise Exception, "invalid cmd %s" % cmd
        
        return int(ret in expected)

    def add(self, key, val, expire=0):
        return self._store('add', key, val, expire)

    def replace(self, key, val, expire=0):
        return self._store('replace', key, val, expire)

    def cas(self, key, val, expire=0, cas=0):
        return self._store('cas', key, val, expire, cas)

    def append(self, key, val):
        return self._store('append', key, val)

    def prepend(self, key, val):
        return self._store('prepend', key, val)

    def incr(self, key, val=1):
        new_value = ctypes.c_uint64(0)
        rc = c.memcached_increment(self.mc, key, len(key), val, ctypes.byref(new_value))
        if rc == MEMCACHED_SUCCESS:
            return new_value.value

    def decr(self, key, val=1):
        new_value = ctypes.c_uint64(0)
        rc = c.memcached_decrement(self.mc, key, len(key), val, ctypes.byref(new_value))
        if rc == MEMCACHED_SUCCESS:
            return new_value.value

    def _store_multi(self, cmd, keys, val):
        if type(val) != type(''):
            sys.stderr.write("[cmemcached]%s only support string: %s" % (cmd, key))
            return 0

        self.set_behavior(BEHAVIOR_NOREPLY, 1)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 1)
        for key in keys:
            if cmd == 'append':
                c.memcached_append(self.mc, key, len(key), val, len(val), 0, 0)
            elif cmd == 'prepend':
                c.memcached_prepend(self.mc, key, len(key), val, len(val), 0, 0)
        retval = c.memcached_flush_buffers(self.mc)
        self.set_behavior(BEHAVIOR_NOREPLY, 0)
        self.set_behavior(BEHAVIOR_BUFFER_REQUESTS, 0)

        return retval == MEMCACHED_SUCCESS

    def append_multi(self, keys, val):
        return self._store_multi('append', keys, val)

    def prepend_multi(self, keys, val):
        return self._store_multi('prepend', keys, val)

    def stats(self):
        rc = ctypes.c_int(0)
        stat = c.memcached_stat(self.mc, None, ctypes.byref(rc))
        if not stat:
            return {}

        stats = {}
        for i in range(len(self.servers)):
            st = {}

            st['pid'] = stat[i].pid
            st['uptime'] = stat[i].uptime
            st['time'] = stat[i].time
            st['pointer_size'] = stat[i].pointer_size
            st['threads'] = stat[i].threads
            st['version'] = stat[i].version

            st['rusage_user'] = stat[i].rusage_system_seconds + stat[i].rusage_system_microseconds / 1e6
            st['rusage_system'] = stat[i].rusage_user_seconds + stat[i].rusage_user_microseconds / 1e6

            st['curr_items'] = stat[i].curr_items
            st['total_items'] = stat[i].total_items

            st['curr_connections'] = stat[i].pid
            st['total_connections'] = stat[i].pid
            st['connection_structures'] = stat[i].pid

            st['cmd_get'] = stat[i].cmd_get
            st['cmd_set'] = stat[i].cmd_set
            st['get_hits'] = stat[i].get_hits
            st['get_misses'] = stat[i].get_misses
            st['evictions'] = stat[i].evictions

            st['bytes'] = stat[i].bytes
            st['bytes_read'] = stat[i].bytes_read
            st['bytes_written'] = stat[i].bytes_written
            st['limit_maxbytes'] = stat[i].limit_maxbytes

            stats[self.servers[i]] = st

        c.memcached_stat_free(self.mc, stat)

        return stats
