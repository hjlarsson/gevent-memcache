from geventmemcache import MemcacheError, MemcacheResult, MemcacheBinResult
from geventmemcache.codec import MemcacheCodec
import struct

class MemcacheProtocol(object):
    @classmethod
    def create(cls, type_):

        if isinstance(type_, MemcacheProtocol):
            return type_
        elif type_ == 'text':
            return MemcacheTextProtocol()
        elif type_ == 'binary':
            return MemcacheBinaryProtocol()
        else:
            raise MemcacheError("unknown protocol: %s" % type_)

class MemcacheTextProtocol(MemcacheProtocol):
    MAX_KEY_LENGTH = 250
    MAX_VALUE_LENGTH = 1024*1024

    def __init__(self, codec = "default"):
        self.set_codec(codec)

    def _check_keys(self, keys):
        for key in keys:
            if not isinstance(key, str):
                raise MemcacheError("Key must be string and not unicode or anything else")

            if len(key) > MemcacheTextProtocol.MAX_KEY_LENGTH:
                raise MemcacheError("Key is bigger then %d" % MemcacheTextProtocol.MAX_KEY_LENGTH)

            # Check so key don't contain whitespace or control characters
            for c in key:
                if ord(c) < 33 or ord(c) == 127:
                    raise MemcacheError("Key contains whitespace or control characters")

    def _check_value(self, value):
        if len(value) > MemcacheTextProtocol.MAX_VALUE_LENGTH:
            raise MemcacheError("Value can't be bigger then %d" % MemcacheTextProtocol.MAX_VALUE_LENGTH)

    def write_stats(self, writer):
        writer.write_bytes("stats\r\n")

    def read_stats(self, reader):
        response_line = reader.read_line()

        result = {}
        while True:
            response_line = reader.read_line()
            if response_line.startswith('STAT'):
                response_fields = response_line.split(' ')
                key = response_fields[1]
                value = response_fields[2]
                result[key] = value
            elif response_line == 'END':
                return MemcacheResult.OK, result
            else:
                return MemcacheResult.get(response_line), {}

        return MemcacheResult.OK, response_line

#        if response_line.startswith('VERSION'):
#            return MemcacheResult.OK, response_line[8:].strip()
#        else:
#            return MemcacheResult.get(response_line), None

    def set_codec(self, codec):
        self._codec = MemcacheCodec.create(codec)

    def _read_result(self, reader, value = None):
        response_line = reader.read_line()
        return MemcacheResult.get(response_line), value

    def write_version(self, writer):
        writer.write_bytes("version\r\n")

    def read_version(self, reader):
        response_line = reader.read_line()
        if response_line.startswith('VERSION'):
            return MemcacheResult.OK, response_line[8:].strip()
        else:
            return MemcacheResult.get(response_line), None

    def _write_storage(self, writer, cmd, key, value, expiration, flags, cas_unique = None):
        self._check_keys([key])
        encoded_value, flags = self._codec.encode(value, flags)
        self._check_value(encoded_value)
        if cas_unique is not None:
            writer.write_bytes("%s %s %d %d %d %d\r\n%s\r\n" % (cmd, key, flags, expiration, len(encoded_value), cas_unique, encoded_value))
        else:
            writer.write_bytes("%s %s %d %d %d\r\n%s\r\n" % (cmd, key, flags, expiration, len(encoded_value), encoded_value))

    def write_cas(self, writer, key, value, expiration, flags, cas_unique):
        self._write_storage(writer, "cas", key, value, expiration, flags, cas_unique)

    def read_cas(self, reader):
        return self._read_result(reader)

    def _write_incdec(self, writer, cmd, key, value):
        self._check_keys([key])
        writer.write_bytes("%s %s %s\r\n" % (cmd, key, value))

    def _read_incdec(self, reader):
        response_line = reader.read_line()
        try:
            return MemcacheResult.OK, int(response_line)
        except ValueError:
            return MemcacheResult.get(response_line), None

    def write_incr(self, writer, key, value):
        self._write_incdec(writer, "incr", key, value)

    def read_incr(self, reader):
        return self._read_incdec(reader)

    def write_decr(self, writer, key, value):
        self._write_incdec(writer, "decr", key, value)

    def read_decr(self, reader):
        return self._read_incdec(reader)

    def write_get(self, writer, keys):
        self._check_keys(keys)
        writer.write_bytes("get %s\r\n" % " ".join(keys))

    def write_gets(self, writer, keys):
        self._check_keys(keys)
        writer.write_bytes("gets %s\r\n" % " ".join(keys))

    def read_get(self, reader, with_cas_unique = False):
        result = {}
        while True:
            response_line = reader.read_line()
            if response_line.startswith('VALUE'):
                response_fields = response_line.split(' ')
                key = response_fields[1]
                flags = int(response_fields[2])
                n = int(response_fields[3])
                if with_cas_unique:
                    cas_unique = int(response_fields[4])
                encoded_value = reader.read_bytes(n)
                reader.read_line() #\r\n
                if with_cas_unique:
                    result[key] = (self._codec.decode(flags, encoded_value), cas_unique)
                else:
                    result[key] = self._codec.decode(flags, encoded_value)
            elif response_line == 'END':
                return MemcacheResult.OK, result
            else:
                return MemcacheResult.get(response_line), {}

    def read_gets(self, reader):
        return self.read_get(reader, with_cas_unique = True)

    def write_delete(self, writer, key, expiration):
        self._check_keys([key])
        writer.write_bytes("delete %s %d\r\n" % (key, expiration))

    def read_delete(self, reader):
        return self._read_result(reader)

    def write_set(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, "set", key, value, expiration, flags)

    def read_set(self, reader):
        return self._read_result(reader)

    def write_add(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, "add", key, value, expiration, flags)

    def read_add(self, reader):
        return self._read_result(reader)

    def write_replace(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, "replace", key, value, expiration, flags)

    def read_replace(self, reader):
        return self._read_result(reader)

    def write_append(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, "append", key, value, expiration, flags)

    def read_append(self, reader):
        return self._read_result(reader)

    def write_prepend(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, "prepend", key, value, expiration, flags)

    def read_prepend(self, reader):
        return self._read_result(reader)

## Magic Byte - See section 3.1
PROTOCOL_BINARY_REQUEST = 0x80 # Request packet for this protocol version
PROTOCOL_BINARY_RESPONSE = 0x81 # Response packet for this protocol version

## Command Opcodes - See section 3.3
PROTOCOL_BINARY_CMD_GET = 0x00
PROTOCOL_BINARY_CMD_SET = 0x01
PROTOCOL_BINARY_CMD_ADD = 0x02
PROTOCOL_BINARY_CMD_REPLACE = 0x03
PROTOCOL_BINARY_CMD_DELETE = 0x04
PROTOCOL_BINARY_CMD_INCREMENT = 0x05
PROTOCOL_BINARY_CMD_DECREMENT = 0x06
PROTOCOL_BINARY_CMD_QUIT = 0x07
PROTOCOL_BINARY_CMD_FLUSH = 0x08
PROTOCOL_BINARY_CMD_GETQ = 0x09
PROTOCOL_BINARY_CMD_NOOP = 0x0a
PROTOCOL_BINARY_CMD_VERSION = 0x0b
PROTOCOL_BINARY_CMD_GETK = 0x0c
PROTOCOL_BINARY_CMD_GETKQ = 0x0d
PROTOCOL_BINARY_CMD_APPEND = 0x0e
PROTOCOL_BINARY_CMD_PREPEND = 0x0f
PROTOCOL_BINARY_CMD_STAT = 0x10
PROTOCOL_BINARY_CMD_SETQ = 0x11
PROTOCOL_BINARY_CMD_ADDQ = 0x12
PROTOCOL_BINARY_CMD_REPLACEQ = 0x13
PROTOCOL_BINARY_CMD_DELETEQ = 0x14
PROTOCOL_BINARY_CMD_INCREMENTQ = 0x15
PROTOCOL_BINARY_CMD_DECREMENTQ = 0x16
PROTOCOL_BINARY_CMD_QUITQ = 0x17
PROTOCOL_BINARY_CMD_FLUSHQ = 0x18
PROTOCOL_BINARY_CMD_APPENDQ = 0x19
PROTOCOL_BINARY_CMD_PREPENDQ = 0x1a
PROTOCOL_BINARY_CMD_VERBOSITY = 0x1b
PROTOCOL_BINARY_CMD_TOUCH = 0x1c
PROTOCOL_BINARY_CMD_GAT = 0x1d
PROTOCOL_BINARY_CMD_GATQ = 0x1e

PROTOCOL_BINARY_CMD_SASL_LIST_MECHS = 0x20
PROTOCOL_BINARY_CMD_SASL_AUTH = 0x21
PROTOCOL_BINARY_CMD_SASL_STEP = 0x22

PROTOCOL_BINARY_CMD_RGET      = 0x30
PROTOCOL_BINARY_CMD_RSET      = 0x31
PROTOCOL_BINARY_CMD_RSETQ     = 0x32
PROTOCOL_BINARY_CMD_RAPPEND   = 0x33
PROTOCOL_BINARY_CMD_RAPPENDQ  = 0x34
PROTOCOL_BINARY_CMD_RPREPEND  = 0x35
PROTOCOL_BINARY_CMD_RPREPENDQ = 0x36
PROTOCOL_BINARY_CMD_RDELETE   = 0x37
PROTOCOL_BINARY_CMD_RDELETEQ  = 0x38
PROTOCOL_BINARY_CMD_RINCR     = 0x39
PROTOCOL_BINARY_CMD_RINCRQ    = 0x3a
PROTOCOL_BINARY_CMD_RDECR     = 0x3b
PROTOCOL_BINARY_CMD_RDECRQ    = 0x3c

PROTOCOL_BINARY_CMD_SET_VBUCKET = 0x3d
PROTOCOL_BINARY_CMD_GET_VBUCKET = 0x3e
PROTOCOL_BINARY_CMD_DEL_VBUCKET = 0x3f

PROTOCOL_BINARY_CMD_TAP_CONNECT = 0x40
PROTOCOL_BINARY_CMD_TAP_MUTATION = 0x41
PROTOCOL_BINARY_CMD_TAP_DELETE = 0x42
PROTOCOL_BINARY_CMD_TAP_FLUSH = 0x43
PROTOCOL_BINARY_CMD_TAP_OPAQUE = 0x44
PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET = 0x45
PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START = 0x46
PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END = 0x47
PROTOCOL_BINARY_CMD_LAST_RESERVED = 0xef
PROTOCOL_BINARY_CMD_SCRUB = 0xf0

## Data Types - See section 3.4
PROTOCOL_BINARY_RAW_BYTES = 0x00 # Raw bytes

PROTOCOL_BINARY_HEADER_FORMAT = "!BBHBBHIIQ" # Header format for binary protocol
PROTOCOL_BINARY_READ_EXRAS_FORMAT = "!I" # format for extra field with reading operation
PROTOCOL_BINARY_WRITE_EXRAS_FORMAT = "!II" # format for extra field with writing operation

class MemcacheBinaryProtocol(MemcacheTextProtocol):
    HEADER_LENGTH = struct.calcsize(PROTOCOL_BINARY_HEADER_FORMAT)

    def __init__(self, codec = "raw"):
        self.set_codec(codec)

    def _unpack_header(self, header):
        header_vars = ('magic', 'opcode', 'key_length', 'extra_length', 'data_type', 'status', 'total_body', 'opaque', "cas")
        return dict(zip(header_vars, struct.unpack(PROTOCOL_BINARY_HEADER_FORMAT, header)))

    def _check_keys(self, keys):
        for key in keys:
            if len(key) > MemcacheBinaryProtocol.MAX_KEY_LENGTH:
                raise MemcacheError("Key is bigger then %d" % MemcacheBinaryProtocol.MAX_KEY_LENGTH)

    def _read_result(self, reader, value = None):
        response_line = reader.read_bytes_available()
        header = self._unpack_header(response_line[:24])
        return MemcacheBinResult.get(header['status']), value

    def _write_storage(self, writer, cmd, key, value, extras, cas_unique = None):
        header = struct.pack(
            PROTOCOL_BINARY_HEADER_FORMAT,
            PROTOCOL_BINARY_REQUEST,
            cmd,
            len(key),
            len(extras),
            PROTOCOL_BINARY_RAW_BYTES,
            0x00,
            len(str(key)) + len(str(value)) + len(extras),
            0x00,
            0x00
        )

        writer.write_bytes("%s%s%s%s" % (header, extras, key, value))

    def write_incr(self, writer, key, value):
        extras = struct.pack(
            "!QQI", value, 0, 0
        )
        self._write_storage(writer, PROTOCOL_BINARY_CMD_INCREMENT, key, "", extras, "")

    def read_incr(self, reader):
        return self._read_result(reader)

    def write_decr(self, writer, key, value):
        extras = struct.pack(
            "!QQI", value, 0, 0
        )
        self._write_storage(writer, PROTOCOL_BINARY_CMD_DECREMENT, key, "", extras, "")

    def read_decr(self, reader):
        return self._read_result(reader)

    def write_get(self, writer, keys):
        ## FIXME: 
        # for key in keys[:-1]:
        #     self._write_storage(writer, PROTOCOL_BINARY_CMD_GETKQ, key, "", "")

        self._write_storage(writer, PROTOCOL_BINARY_CMD_GETK, keys[-1], "", "")

    def read_get(self, reader, with_cas_unique = False):
        result = {}

        while True:
            response_line = reader.read_bytes_available()
            header = self._unpack_header(response_line[:24])
            mem_result = MemcacheBinResult.get(header['status'])

            if mem_result == MemcacheResult.OK:
                key_begin = MemcacheBinaryProtocol.HEADER_LENGTH + header['extra_length']
                key_end = key_begin + header['key_length']
                key = response_line[key_begin:key_end]
                value = response_line[key_end:]
                result[key] = value
                return MemcacheResult.OK, result
            elif mem_result == MemcacheResult.NOT_FOUND:
                return MemcacheBinResult.OK, {}

    def write_delete(self, writer, key, expiration):
        self._check_keys([key])
        self._write_storage(writer, PROTOCOL_BINARY_CMD_DELETE, key, "", "")

    def read_delete(self, reader):
        return self._read_result(reader)

    def write_set(self, writer, key, value, expiration, flags):
        extras = struct.pack(
            PROTOCOL_BINARY_WRITE_EXRAS_FORMAT,
            flags,
            expiration
        )
        return self._write_storage(writer, PROTOCOL_BINARY_CMD_SET, key, value, extras)

    def read_set(self, reader):
        return self._read_result(reader)

    def write_add(self, writer, key, value, expiration, flags):
        extras = struct.pack(
            PROTOCOL_BINARY_WRITE_EXRAS_FORMAT,
            flags,
            expiration
        )
        return self._write_storage(writer, PROTOCOL_BINARY_CMD_ADD, key, value, extras)

    def read_add(self, reader):
        return self._read_result(reader)

    def write_replace(self, writer, key, value, expiration, flags):
        extras = struct.pack(
            PROTOCOL_BINARY_WRITE_EXRAS_FORMAT,
            flags,
            expiration
        )
        return self._write_storage(writer, PROTOCOL_BINARY_CMD_REPLACE, key, value, extras)

    def read_replace(self, reader):
        return self._read_result(reader)

    def write_append(self, writer, key, value, expiration, flags):
       return self._write_storage(writer, PROTOCOL_BINARY_CMD_APPEND, key, value, "")

    def read_append(self, reader):
        return self._read_result(reader)

    def write_prepend(self, writer, key, value, expiration, flags):
        return self._write_storage(writer, PROTOCOL_BINARY_CMD_PREPEND, key, value, "")

    def read_prepend(self, reader):
        return self._read_result(reader)

    def write_stats(self, writer):
        return self._write_storage(writer, PROTOCOL_BINARY_CMD_STAT, "", "", "")

    def read_stats(self, reader):
        result = {}

        while True:
            response_line = reader.read_bytes_available()
            header = self._unpack_header(response_line[:24])
            mem_result = MemcacheBinResult.get(header['status'])
            
            if mem_result == MemcacheResult.OK:
                key_begin = MemcacheBinaryProtocol.HEADER_LENGTH + header['extra_length']
                key_end = key_begin + header['key_length']
                val_end = key_end + header['total_body'] - header['key_length']
                key = response_line[key_begin:key_end]
                value = response_line[key_end:val_end]
                result[key] = value
                return MemcacheResult.OK, result
            else:
                return MemcacheBinResult.ERROR, {}

    def write_cas(self, writer, key, value, expiration, flags):
        raise NotImplementedError('"cas" is not implemented.')

    def write_gets(self, writer, key, value, expiration, flags):
        raise NotImplementedError('"gets" is not implemented.')

