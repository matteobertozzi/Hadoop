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

import struct
import os

class InputStream(object):
    def available(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def mark(self, read_limit):
        raise NotImplementedError

    def markSupported(self):
        raise NotImplementedError

    def readByte(self):
        return self.read(1)

    def readFully(self, length):
        return self.read(length)

    def read(self, length):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def skip(self, n):
        raise NotImplementedError

class ByteArrayInputStream(InputStream):
    def __init__(self, data='', offset=0, length=0):
        self.reset(data, offset, length)

    def size(self):
        return self._count - self._offset

    def toByteArray(self):
        return self._buffer[self._offset:self._offset+self._count]

    def reset(self, data, offset=0, length=0):
        if data and not length:
            length = len(data) - offset
        self._buffer = data
        self._offset = offset
        self._count = length

    def close(self):
        pass

    def flush(self):
        pass

    def read(self, length):
        data = self._buffer[self._offset:self._offset+length]
        self._offset += length
        return data

class FileInputStream(InputStream):
    def __init__(self, path):
        self._fd = open(path, 'rb')
        self._length = os.path.getsize(path)

    def length(self):
        return self._length

    def close(self):
        self._fd.close()

    def seek(self, offset):
        self._fd.seek(offset)

    def getPos(self):
        return self._fd.tell()

    def readByte(self):
        return self._fd.read(1)

    def read(self, length):
        byte_buffer = []
        while length > 0:
            data = self._fd.read(length)
            if not data:
                break

            data_length = len(data)
            byte_buffer.append(data)
            length -= data_length
        return ''.join(byte_buffer)

    def skip(self, n):
        skip_length = 0
        while n > 0:
            data = self._fd.read(n)
            if not data:
                break

            data_length = len(data)
            skip_length += data_length
            n -= data_length
        return skip_length

class DataInputStream(InputStream):
    def __init__(self, input_stream):
        assert isinstance(input_stream, InputStream)
        self._stream = input_stream

    def close(self):
        return self._stream.close()

    def seek(self, offset):
        return self._stream.seek(offset)

    def getPos(self):
        return self._stream.getPos()

    def length(self):
        return self._stream.length()

    def read(self, length):
        return self._stream.read(length)

    def readByte(self):
        data = self._stream.read(1)
        return struct.unpack(">b", data)[0]

    def readFully(self, length):
        return [self.readByte() for _ in xrange(length)]

    def readUByte(self):
        data = self._stream.read(1)
        return struct.unpack("B", data)[0]

    def readBoolean(self):
        data = self._stream.read(1)
        return struct.unpack(">?", data)[0]

    def readInt(self):
        data = self._stream.read(4)
        return struct.unpack(">i", data)[0]

    def readLong(self):
        data = self._stream.read(8)
        return struct.unpack(">q", data)[0]

    def readFloat(self):
        data = self._stream.read(4)
        return struct.unpack(">f", data)[0]

    def readDouble(self):
        data = self._stream.read(8)
        return struct.unpack(">d", data)[0]

    def skipBytes(self, n):
        return self._stream.skip(n)

class DataInputBuffer(DataInputStream):
    def __init__(self, data='', offset=0, length=0):
        input_stream = ByteArrayInputStream(data, offset, length)
        super(DataInputBuffer, self).__init__(input_stream)

    def reset(self, data, offset=0, length=0):
        self._stream.reset(data, offset, length)

    def size(self):
        return self._stream.size()

    def toByteArray(self):
        return self._stream.toByteArray()

