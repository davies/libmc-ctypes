import ctypes
from ctypes import *

c_time_t = c_long

class memcached_st(ctypes.Structure):
    _fields_ = []
memcached_st_p = ctypes.POINTER(memcached_st)

class memcached_server_st(ctypes.Structure):
    _fields_ = []
memcached_server_list_st = memcached_server_st_p = POINTER(memcached_server_st)

class memcached_stat_st(ctypes.Structure):
    _fields_ = [
        ('connection_structures', c_uint32),
        ('curr_connections', c_uint32),
        ('curr_items', c_uint32),
        ('pid', c_uint32),
        ('pointer_size', c_uint32),
        ('rusage_system_microseconds', c_uint32),
        ('rusage_system_seconds', c_uint32),
        ('rusage_user_microseconds', c_uint32),
        ('rusage_user_seconds', c_uint32),
        ('threads', c_uint32),
        ('time', c_uint32),
        ('total_connections', c_uint32),
        ('total_items', c_uint32),
        ('uptime', c_uint32),
        ('bytes', c_uint64),
        ('bytes_read', c_uint64),
        ('bytes_written', c_uint64),
        ('cmd_get', c_uint64),
        ('cmd_set', c_uint64),
        ('evictions', c_uint64),
        ('get_hits', c_uint64),
        ('get_misses', c_uint64),
        ('limit_maxbytes', c_uint64),
        ('version', c_char * 24),
        ('root', memcached_st_p),
    ]
memcached_stat_st_p = POINTER(memcached_stat_st)

try:
    c = ctypes.CDLL("libmemcached.so")
except:
    raise ImportError("Can't find libmemcached.so")

c.memcached_create.argtypes = [memcached_st_p]
c.memcached_create.restype = memcached_st_p

c.memcached_free.argtypes = [memcached_st_p]
c.memcached_free.restype = None

c.memcached_server_count.argtypes = [memcached_st_p]
c.memcached_server_count.restype = c_uint

c.memcached_servers_parse.argtypes = [c_char_p]
c.memcached_servers_parse.restype = memcached_server_st_p

c.memcached_server_push.argtypes = [memcached_st_p, memcached_server_st_p]
c.memcached_server_push.restype = c_int

c.memcached_server_list_free.argtypes = [memcached_server_st_p]
c.memcached_server_list_free.restype = None

c.memcached_generate_hash.argtypes = [memcached_st_p, c_char_p, c_size_t]
c.memcached_generate_hash.restype = c_uint32

c.memcached_behavior_set.argtypes = [memcached_st_p, c_int, c_uint64]
c.memcached_behavior_set.restype = c_int

c.memcached_behavior_get.argtypes = [memcached_st_p, c_int]
c.memcached_behavior_get.restype = c_uint64

c.memcached_set.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32] 
c.memcached_set.restype = c_int

c.memcached_add.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32] 
c.memcached_add.restype = c_int

c.memcached_replace.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32] 
c.memcached_replace.restype = c_int

c.memcached_cas.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32, c_uint64]
c.memcached_cas.restype = c_int

c.memcached_append.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32] 
c.memcached_append.restype = c_int

c.memcached_prepend.argtypes = [memcached_st_p, c_char_p, c_size_t, POINTER(c_char), c_size_t, c_time_t, c_uint32] 
c.memcached_prepend.restype = c_int

c.memcached_increment.argtypes = [memcached_st_p, c_char_p, c_size_t, c_uint32, POINTER(c_uint64)]
c.memcached_increment.restype = c_int

c.memcached_decrement.argtypes = [memcached_st_p, c_char_p, c_size_t, c_uint32, POINTER(c_uint64)]
c.memcached_decrement.restype = c_int

c.memcached_flush_buffers.argtypes = [memcached_st_p]
c.memcached_flush_buffers.restype = c_int

c.memcached_get.argtypes = [memcached_st_p, c_char_p, c_int, POINTER(c_size_t), POINTER(c_uint32), POINTER(c_int)]
c.memcached_get.restype = POINTER(c_char)

c.memcached_mget.argtypes = [memcached_st_p, POINTER(c_char_p), POINTER(c_size_t), c_int]
c.memcached_mget.restype = c_int

c.memcached_fetch.argtypes = [memcached_st_p, c_char_p, POINTER(c_size_t), POINTER(c_size_t), POINTER(c_uint32), POINTER(c_int)]
c.memcached_fetch.restype = POINTER(c_char)

c.memcached_delete.argtypes = [memcached_st_p, c_char_p, c_size_t, c_time_t]
c.memcached_delete.restype = c_int

c.memcached_stat.argtypes = [memcached_st_p, c_char_p, POINTER(c_int)]
c.memcached_stat.restype = memcached_stat_st_p

c.memcached_stat_free.argtypes = [memcached_st_p, memcached_stat_st_p]
c.memcached_stat_free.restype = None
