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

class OutputStream(object):
    def close(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def writeByte(self, byte):
        return self.writeFully(str(byte))

    def write(self, data):
        raise NotImplementedError

class FileOutputStream(OutputStream):
    def __init__(self, path):
        self._fd = open(path, 'wb')

    def close(self):
        self._fd.close()

    def seek(self, offset):
        self._fd.seek(offset)

    def flush(self):
        return self._fd.flush()

    def getPos(self):
        return self._fd.tell()

    def writeByte(self, value):
        return self._fd.write(value)

    def write(self, value):
        return self._fd.write(value)

class DataOutputStream(object):
    def __init__(self, output_stream):
        assert isinstance(output_stream, OutputStream)
        self._stream = output_stream

    def close(self):
        return self._stream.close()

    def seek(self, offset):
        return self._stream.seek(offset)

    def getPos(self):
        return self._stream.getPos()

    def write(self, length):
        return self._stream.write(length)

    def writeByte(self, value):
        data = struct.pack(">b", value)
        assert len(data) == 1
        return self._stream.write(data)

    def writeUByte(self, value):
        data = struct.pack("B", value)
        assert len(data) == 1
        return self._stream.write(data)

    def writeBoolean(self, value):
        data = struct.pack(">?", value)
        assert len(data) == 1
        return self._stream.write(data)

    def writeInt(self, value):
        data = struct.pack(">i", value)
        assert len(data) == 4
        return self._stream.write(data)

    def writeLong(self, value):
        data = struct.pack(">q", value)
        assert len(data) == 8
        return self._stream.write(data)

    def writeFloat(self, value):
        data = struct.pack(">f", value)
        assert len(data) == 4
        return self._stream.write(data)

    def writeDouble(self, value):
        data = struct.pack(">d", value)
        assert len(data) == 8
        return self._stream.write(data)

    def skipBytes(self, n):
        return self._stream.skip(n)

class ByteArrayOutputStream(OutputStream):
    def __init__(self):
        self._buffer = []
        self._count = 0

    def size(self):
        return self._count

    def toByteArray(self):
        return ''.join(self._buffer)

    def reset(self):
        self._buffer = []
        self._count = 0

    def close(self):
        pass

    def flush(self):
        pass

    def write(self, bytes):
        self._buffer.append(bytes)
        self._count += len(bytes)

class DataOutputBuffer(DataOutputStream):
    def __init__(self):
        super(DataOutputBuffer, self).__init__(ByteArrayOutputStream())

    def getData(self):
        return self._stream.toByteArray()

    def getSize(self):
        return self._stream.size()

    def reset(self):
        self._stream.reset()

    def writeStreamData(self, input_stream, length):
        self._stream.write(input_stream.read(length))

    def toByteArray(self):
        return self._stream.toByteArray()

