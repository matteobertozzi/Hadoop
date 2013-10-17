#!/usr/bin/env python
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from hashlib import md5
from uuid import uuid1
from time import time
import os

from hadoop.util.ReflectionUtils import hadoopClassFromName, hadoopClassName

from compress import CodecPool

from WritableUtils import readVInt, writeVInt
from Writable import Writable
from OutputStream import FileOutputStream, DataOutputStream, DataOutputBuffer
from InputStream import FileInputStream, DataInputStream, DataInputBuffer
from VersionMismatchException import VersionMismatchException, VersionPrefixException

from Text import Text

BLOCK_COMPRESS_VERSION  = '\x04'
CUSTOM_COMPRESS_VERSION = '\x05'
VERSION_WITH_METADATA   = '\x06'
VERSION_PREFIX = 'SEQ'
VERSION = VERSION_PREFIX + VERSION_WITH_METADATA

SYNC_ESCAPE = -1
SYNC_HASH_SIZE = 16
SYNC_SIZE = 4 + SYNC_HASH_SIZE

SYNC_INTERVAL = 100 * SYNC_SIZE

class CompressionType:
    NONE = 0
    RECORD = 1
    BLOCK  = 2

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

    def keys(self):
        return self._meta.keys()

    def iterkeys(self):
        return self._meta.iterkeys()

    def values(self):
        return self._meta.values()

    def itervalues(self):
        return self._meta.itervalues()

    def iteritems(self):
        return self._meta.iteritems()

    def __iter__(self):
        return self._meta.iteritems()

    def write(self, data_output):
        data_output.writeInt(len(self._meta))
        for key, value in self._meta.iteritems():
            Text.writeString(data_output, key)
            Text.writeString(data_output, value)

    def readFields(self, data_input):
        count = data_input.readInt()
        if count < 0:
            raise IOError("Invalid size: %d for file metadata object" % count)

        for i in xrange(count):
            key = Text.readString(data_input)
            value = Text.readString(data_input)
            self._meta[key] = value

def createWriter(path, key_class, value_class, metadata=None, compression_type=CompressionType.NONE):
    kwargs = {}

    if compression_type == CompressionType.NONE:
        pass
    elif compression_type == CompressionType.RECORD:
        kwargs['compress'] = True
    elif compression_type == CompressionType.BLOCK:
        kwargs['compress'] = True
        kwargs['block_compress'] = True
    else:
        raise NotImplementedError("Compression Type Not Supported")

    return Writer(path, key_class, value_class, metadata, **kwargs)

def createRecordWriter(path, key_class, value_class, metadata=None):
    return Writer(path, key_class, value_class, metadata, compress=True)

def createBlockWriter(path, key_class, value_class, metadata=None):
    return Writer(path, key_class, value_class, metadata, compress=True, block_compress=True)

class Writer(object):
    COMPRESSION_BLOCK_SIZE = 1000000

    def __init__(self, path, key_class, value_class, metadata, compress=False, block_compress=False):
        if os.path.exists(path):
            raise IOError("File %s already exists." % path)

        self._key_class = key_class
        self._value_class = value_class
        self._compress = compress
        self._block_compress = block_compress

        if not metadata:
            metadata = Metadata()
        self._metadata = metadata

        if self._compress or self._block_compress:
            self._codec = CodecPool().getCompressor()
        else:
            self._codec = None

        self._last_sync = 0
        self._block = None

        self._stream = DataOutputStream(FileOutputStream(path))

        # sync is 16 random bytes
        self._sync = md5('%s@%d' % (uuid1().bytes, int(time() * 1000))).digest()

        self._writeFileHeader()

    def close(self):
        if self._block_compress:
            self.sync()
        self._stream.close()

    def getCompressionCodec(self):
        return self._codec

    def getKeyClass(self):
        return self._key_class

    def getKeyClassName(self):
        return hadoopClassName(self._key_class)

    def getValueClass(self):
        return self._value_class

    def getValueClassName(self):
        return hadoopClassName(self._value_class)

    def isBlockCompressed(self):
        return self._block_compress

    def isCompressed(self):
        return self._compress

    def getLength(self):
        return self._stream.getPos()

    def append(self, key, value):
        if type(key) != self._key_class:
            raise IOError("Wrong key class %s is not %s" % (type(key), self._key_class))

        if type(value) != self._value_class:
            raise IOError("Wrong Value class %s is not %s" % (type(value), self._value_class))

        key_buffer = DataOutputBuffer()
        key.write(key_buffer)

        value_buffer = DataOutputBuffer()
        value.write(value_buffer)

        self.appendRaw(key_buffer.toByteArray(), value_buffer.toByteArray())

    def appendRaw(self, key, value):
        if self._block_compress:
            if self._block:
                records, keys_len, keys, values_len, values = self._block
            else:
                keys_len = DataOutputBuffer()
                keys = DataOutputBuffer()
                values_len = DataOutputBuffer()
                values = DataOutputBuffer()
                records = 0

            writeVInt(keys_len, len(key))
            keys.write(key)

            writeVInt(values_len, len(value))
            values.write(value)

            records += 1

            self._block = (records, keys_len, keys, values_len, values)

            current_block_size = keys.getSize() + values.getSize()
            if current_block_size >= self.COMPRESSION_BLOCK_SIZE:
                self.sync()
        else:
            if self._compress:
                value = self._codec.compress(value)

            key_length = len(key)
            value_length = len(value)

            self._checkAndWriteSync()
            self._stream.writeInt(key_length + value_length)
            self._stream.writeInt(key_length)
            self._stream.write(key)
            self._stream.write(value)

    def sync(self):
        if self._last_sync != self._stream.getPos():
            self._stream.writeInt(SYNC_ESCAPE)
            self._stream.write(self._sync)
            self._last_sync = self._stream.getPos()

        if self._block_compress and self._block:
            def _writeBuffer(data_buf):
                buf = self._codec.compress(data_buf.toByteArray())
                writeVInt(self._stream, len(buf))
                self._stream.write(buf)

            records, keys_len, keys, values_len, values = self._block

            writeVInt(self._stream, records)

            _writeBuffer(keys_len)
            _writeBuffer(keys)

            _writeBuffer(values_len)
            _writeBuffer(values)

            self._block = None

    def _writeFileHeader(self):
        self._stream.write(VERSION)
        Text.writeString(self._stream, self.getKeyClassName())
        Text.writeString(self._stream, self.getValueClassName())

        self._stream.writeBoolean(self._compress)
        self._stream.writeBoolean(self._block_compress)

        if self._codec:
            Text.writeString(self._stream, 'org.apache.hadoop.io.compress.DefaultCodec')

        self._metadata.write(self._stream)
        self._stream.write(self._sync)

    def _checkAndWriteSync(self):
        if self._stream.getPos() >= (self._last_sync + SYNC_INTERVAL):
            self.sync()

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

    def getStream(self, path):
        return DataInputStream(FileInputStream(path))

    def close(self):
        self._stream.close()

    def getCompressionCodec(self):
        return self._codec

    def getKeyClass(self):
        if not self._key_class:
          self._key_class = hadoopClassFromName(self._key_class_name)
        return self._key_class

    def getKeyClassName(self):
        return hadoopClassName(self.getKeyClass())

    def getValueClass(self):
        if not self._value_class:
          self._value_class = hadoopClassFromName(self._value_class_name)
        return self._value_class

    def getValueClassName(self):
        return hadoopClassName(self.getValueClass())

    def getPosition(self):
        return self._stream.getPos()

    def getMetadata(self):
        return self._metadata

    def isBlockCompressed(self):
        return self._block_compressed

    def isCompressed(self):
        return self._decompress

    def nextRawKey(self):
        if not self._block_compressed:
            record_length = self._readRecordLength()
            if record_length < 0:
                return None

            key_length = self._stream.readInt()
            key = DataInputBuffer(self._stream.read(key_length))
            self._record.reset(self._stream.read(record_length - key_length))
            return key
        else:
            if hasattr(self, '_block_index') and \
               self._block_index < self._record[0]:
                self._sync_seen = False
                records, keys_len, keys, values_len, values = self._record
                key_length = readVInt(keys_len)
                self._block_index += 1
                return DataInputBuffer(keys.read(key_length))

            if self._stream.getPos() >= self._end:
                return None

            # Read Sync
            self._stream.readInt() # -1
            sync_check = self._stream.read(SYNC_HASH_SIZE)
            if sync_check != self._sync:
                raise IOError("File is corrupt")
            self._sync_seen = True

            def _readBuffer():
                length = readVInt(self._stream)
                buf = self._stream.read(length)
                return self._codec.decompressInputStream(buf)

            records = readVInt(self._stream)
            keys_len = _readBuffer()
            keys = _readBuffer()

            values_len = _readBuffer()
            values = _readBuffer()

            self._record = (records, keys_len, keys, values_len, values)
            self._block_index = 1

            key_length = readVInt(keys_len)
            return DataInputBuffer(keys.read(key_length))

    def nextKey(self, key):
        buf = self.nextRawKey()
        if not buf:
          return False
        key.readFields(buf)
        return True

    def nextRawValue(self):
        if not self._block_compressed:
            if self._decompress:
                compress_data = self._record.read(self._record.size())
                return self._codec.decompressInputStream(compress_data)
            else:
                return self._record
        else:
            records, keys_len, keys, values_len, values = self._record
            value_length = readVInt(values_len)
            return DataInputBuffer(values.read(value_length))

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
        self._stream = self.getStream(path)

        if length == 0:
            self._end = self._stream.getPos() + self._stream.length()
        else:
            self._end = self._stream.getPos() + length

        # Parse Header
        version_block = self._stream.read(len(VERSION))

        if not version_block.startswith(VERSION_PREFIX):
            raise VersionPrefixException(VERSION_PREFIX,
                                         version_block[0:len(VERSION_PREFIX)])

        self._version = version_block[len(VERSION_PREFIX)]
        if self._version > VERSION[len(VERSION_PREFIX)]:
            raise VersionMismatchException(VERSION[len(VERSION_PREFIX)],
                                           self._version)

        if self._version < BLOCK_COMPRESS_VERSION:
            # Same as below, but with UTF8 Deprecated Class
            raise NotImplementedError
        else:
            self._key_class_name = Text.readString(self._stream)
            self._value_class_name = Text.readString(self._stream)

        if ord(self._version) > 2:
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
        stream = self.nextRawValue()
        value.readFields(stream)
        if not self._block_compressed:
            assert self._record.size() == 0
