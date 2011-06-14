# Copyright (C) 2009, Hyves (Startphone Ltd.)
#
# This module is part of the Concurrence Framework and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
import logging
logging.TRACE = 5

class MemcacheError(Exception):
    pass

class MemcacheResult(object):
    """representation of memcache result (codes)"""

    _interned = {}

    def __init__(self, name, msg = ''):
        self._name = name
        self._msg = msg

    @property
    def msg(self):
        return self._msg

    def __repr__(self):
        return "MemcacheResult.%s" % self._name

    def __eq__(self, other):
        return isinstance(other, MemcacheResult) and other._name == self._name

    @classmethod
    def get(cls, line):
        code = cls._interned.get(line, None)
        if code is None:
            #try client or server error
            if line.startswith('CLIENT_ERROR'):
                return MemcacheResult("CLIENT_ERROR", line[13:])
            elif line.startswith('SERVER_ERROR'):
                return MemcacheResult("SERVER_ERROR", line[13:])
            else:
                raise MemcacheError("unknown response: %s" % repr(line))
        else:
            return code

    @classmethod
    def _intern(cls, name):
        cls._interned[name] = MemcacheResult(name)
        return cls._interned[name]

MemcacheResult.OK = MemcacheResult._intern("OK")
MemcacheResult.STORED = MemcacheResult._intern("STORED")
MemcacheResult.NOT_STORED = MemcacheResult._intern("NOT_STORED")
MemcacheResult.EXISTS = MemcacheResult._intern("EXISTS")
MemcacheResult.NOT_FOUND = MemcacheResult._intern("NOT_FOUND")
MemcacheResult.DELETED = MemcacheResult._intern("DELETED")
MemcacheResult.ERROR = MemcacheResult._intern("ERROR")
MemcacheResult.TIMEOUT = MemcacheResult._intern("TIMEOUT")

## Response Status - See section 3.2
PROTOCOL_BINARY_RESPONSE_SUCCESS = 0x00 # No error
PROTOCOL_BINARY_RESPONSE_KEY_ENOENT = 0x01 # Key not found
PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS = 0x02 # Key exists
PROTOCOL_BINARY_RESPONSE_E2BIG = 0x03 # Value too large
PROTOCOL_BINARY_RESPONSE_EINVAL = 0x04 # Invalid arguments
PROTOCOL_BINARY_RESPONSE_NOT_STORED = 0x05 # Item not stored
PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL = 0x06 # Incr/Decr on non-numeric value.

PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET = 0x07
PROTOCOL_BINARY_RESPONSE_AUTH_ERROR = 0x20
PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE = 0x21
PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND = 0x81 # Unknown command
PROTOCOL_BINARY_RESPONSE_ENOMEM = 0x82 # Out of memory
PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED = 0x83
PROTOCOL_BINARY_RESPONSE_EINTERNAL = 0x84
PROTOCOL_BINARY_RESPONSE_EBUSY = 0x85
PROTOCOL_BINARY_RESPONSE_ETMPFAIL = 0x86

class MemcacheBinResult(MemcacheResult):
    @classmethod
    def get(cls, line):
        code = cls._interned.get(line, None)
        return code

    @classmethod
    def _intern(cls, name, value):
        cls._interned[value] = MemcacheResult(name)
        return MemcacheResult._interned[name]

MemcacheBinResult.OK = MemcacheBinResult._intern("OK", PROTOCOL_BINARY_RESPONSE_SUCCESS)
#MemcacheBinResult.STORED = MemcacheBinResult._intern("STORED", PROTOCOL_BINARY_RESPONSE_SUCCESS)
MemcacheBinResult.NOT_STORED = MemcacheBinResult._intern("NOT_STORED", PROTOCOL_BINARY_RESPONSE_KEY_ENOENT)
MemcacheBinResult.EXISTS = MemcacheBinResult._intern("EXISTS", PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS)
MemcacheBinResult.NOT_FOUND = MemcacheBinResult._intern("NOT_FOUND", PROTOCOL_BINARY_RESPONSE_KEY_ENOENT)
#MemcacheBinResult.DELETED = MemcacheBinResult._intern("DELETED", PROTOCOL_BINARY_RESPONSE_SUCCESS)
#MemcacheBinResult.ERROR = MemcacheBinResult._intern("ERROR", 0)
#MemcacheBinResult.TIMEOUT = MemcacheBinResult._intern("TIMEOUT", 0)

from geventmemcache.client import Memcache, MemcacheConnection, MemcacheConnectionManager
from geventmemcache.behaviour import MemcacheBehaviour
from geventmemcache.protocol import MemcacheProtocol
from geventmemcache.codec import MemcacheCodec
