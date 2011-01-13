#!/usr/bin/env python

from util.ReflectionUtils import hadoopClassFromName

from compress import CodecPool

from WritableUtils import readVInt
from Writable import Writable
from OutputStream import *
from InputStream import *

from Text import Text

BLOCK_COMPRESS_VERSION  = '\x04'
CUSTOM_COMPRESS_VERSION = '\x05'
VERSION_WITH_METADATA   = '\x06'
VERSION = 'SEQ' + VERSION_WITH_METADATA

SYNC_HASH_SIZE = 16
SYNC_SIZE = 4 + SYNC_HASH_SIZE

SYNC_ESCAPE = -1

class Metadata(Writable):
    def __init__(self, metadata=None):
        if metadata:
            self._meta = dict(metadata)
        else:
            self._meta = {}

    def get(self, name):
        return self._meta[name]

    def set(self, name, value):
        self._meta[name] = value

    def write(self, data_output):
        data_output.writeInt(len(self._meta))
        for key, value in self._meta.iteritems():
            key.write(data_output)
            value.write(data_output)

    def readFields(self, data_input):
        count = data_input.readInt()
        if count < 0:
            raise IOError("Invalid size: %d for file metadata object" % count)

        for i in xrange(count):
            key = Text()
            value = Text()
            key.readFields(data_input)
            value.readFields(data_input)
            self._metadata[key] = value

class Writer(object):
    pass

class Reader(object):
    def __init__(self, path, start=0, length=0):
        self._block_compressed = False
        self._decompress = False
        self._sync_seen = False

        self._value_class = None
        self._key_class = None
        self._codec = None

        self._metadata = None

        self._record = DataInputBuffer()

        self._initialize(path, start, length)

    def close(self):
        self._stream.close()

    def getCompressionCodec(self):
        return self._codec

    def getKeyClass(self):
        return self._key_class

    def getKeyClassName(self):
        return self._key_class.__name__

    def getValueClass(self):
        return self._value_class

    def getValueClassName(self):
        return self._value_class.__name__

    def getPosition(self):
        return self._stream.getPos()

    def getMetadata(self):
        return self._metadata

    def isBlockCompressed(self):
        return self._block_compressed

    def isCompressed(self):
        return self._decompress

    def nextKey(self, key):
        if not self._block_compressed:
            record_length = self._readRecordLength()
            if record_length < 0:
                return False

            record_data = self._stream.read(record_length + 4)
            self._record.reset(record_data)

            self._record.readInt() # read key_length
            key.readFields(self._record)
        else:
            if hasattr(self, '_block_index') and \
               self._block_index < self._record[0]:
                self._sync_seen = False
                records, keys_len, keys, values_len, values = self._record
                readVInt(keys_len)
                key.readFields(keys)
                self._block_index += 1
                return True

            if self._stream.getPos() >= self._end:
                return False

            # Read Sync
            self._stream.readInt() # -1
            sync_check = self._stream.read(SYNC_HASH_SIZE)
            if sync_check != self._sync:
                raise IOError("File is corrupt")
            self._sync_seen = True

            def _readBuffer():
                length = readVInt(self._stream)
                buf = self._stream.read(length)

                self._codec.setInput(buf)
                print len(buf), len(self._codec.decompress())
                return self._codec.inputStream()

            records = readVInt(self._stream)
            keys_len = _readBuffer()
            keys = _readBuffer()

            values_len = _readBuffer()
            values = _readBuffer()

            self._record = (records, keys_len, keys, values_len, values)
            self._block_index = 1

            readVInt(keys_len)
            key.readFields(keys)

        return True

    def next(self, key, value):
        more = self.nextKey(key)
        if more:
            self._getCurrentValue(value)
        return more

    def seek(self, position):
        self._stream.seek(position)
        if self._block_compressed:
            self._no_buffered_keys = 0
            self._values_decompressed = True

    def sync(self, position):
        if (position + SYNC_SIZE) > self._end:
            self.seek(self._end)
            return

        if position < self._header_end:
            self._stream.seek(self._header_end)
            self._sync_seen = True
            return

        self.seek(position + 4)
        sync_check = [x for x in self._stream.read(SYNC_HASH_SIZE)]

        i = 0
        while self._stream.getPos() < self._end:
            j = 0
            while j < SYNC_HASH_SIZE:
                if self._sync[j] != sync_check[(i + j) % SYNC_HASH_SIZE]:
                    break
                j += 1

            if j == SYNC_HASH_SIZE:
                self._stream.seek(self._stream.getPos() - SYNC_SIZE)
                return

            sync_check[i % SYNC_HASH_SIZE] = chr(self._stream.readByte())

            i += 1

    def syncSeen(self):
        return self._sync_seen

    def _initialize(self, path, start, length):
        self._stream = DataInputStream(FileInputStream(path))

        if length == 0:
            self._end = self._stream.getPos() + self._stream.length()
        else:
            self._end = self._stream.getPos() + length

        # Parse Header
        version_block = self._stream.read(len(VERSION))

        self._version = version_block[3]
        if self._version > VERSION[3]:
            raise VersionMismatchException(VERSION[3], self._version)

        if self._version < BLOCK_COMPRESS_VERSION:
            # Same as below, but with UTF8 Deprecated Class
            raise NotImplementedError
        else:
            key_class_name = Text.readString(self._stream)
            value_class_name = Text.readString(self._stream)
            self._key_class = hadoopClassFromName(key_class_name)
            self._value_class = hadoopClassFromName(value_class_name)

        if self._version > 2:
            self._decompress = self._stream.readBoolean()
        else:
            self._decompress = False

        if self._version >= BLOCK_COMPRESS_VERSION:
            self._block_compressed = self._stream.readBoolean()
        else:
            self._block_compressed = False

        # setup compression codec
        if self._decompress:
            if self._version >= CUSTOM_COMPRESS_VERSION:
                codec_class = Text.readString(self._stream)
                self._codec = CodecPool().getDecompressor(codec_class)
            else:
                self._codec = CodecPool().getDecompressor()

        self._metadata = Metadata()
        if self._version >= VERSION_WITH_METADATA:
            self._metadata.readFields(self._stream)

        if self._version > 1:
            self._sync = self._stream.read(SYNC_HASH_SIZE)
            self._header_end = self._stream.getPos()

    def _readRecordLength(self):
        if self._stream.getPos() >= self._end:
            return -1

        length = self._stream.readInt()
        if self._version > 1 and self._sync is not None and length == SYNC_ESCAPE:
            sync_check = self._stream.read(SYNC_HASH_SIZE)
            if sync_check != self._sync:
                raise IOError("File is corrupt!")

            self._sync_seen = True
            if self._stream.getPos() >= self._end:
                return -1

            length = self._stream.readInt()
        else:
            self._sync_seen = False

        return length

    def _getCurrentValue(self, value):
        if not self._block_compressed:
            if self._decompress:
                compress_data = self._record.read(self._record.size())
                self._codec.setInput(compress_data)
                value.readFields(self._codec.inputStream())
            else:
                value.readFields(self._record)
            assert self._record.size() == 0
        else:
            records, keys_len, keys, values_len, values = self._record
            value.readFields(values)
