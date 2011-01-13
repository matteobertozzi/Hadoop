#!/usr/bin/env python

import struct

class OutputStream(object):
    def close(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def write(self, bytes, offset, length):
        return self.writeFully(bytes[offset:offset+length])

    def writeByte(self, byte):
        return self.writeFully(str(byte))

    def writeFully(self, bytes):
        raise NotImplementedError

class DataOutputStream(object):
    def __init__(self, output_stream):
        assert isinstance(output_stream, OutputStream)
        self._stream = output_stream

    def seek(self, offset):
        return self._stream.seek(offset)

    def getPos(self):
        return self._stream.getPos()

    def write(self, length):
        return self._stream.write(length)

    def writeByte(self, value):
        return self._stream.writeByte()

    def writeBoolean(self, value):
        data = struct.unpack(">?", value)
        assert len(data) == 1
        return self._stream.write(data)

    def writeInt(self, value):
        data = struct.unpack(">i", value)
        assert len(data) == 4
        return self._stream.write(data)

    def writeLong(self, value):
        data = struct.unpack(">q", value)
        assert len(data) == 8
        return self._stream.write(data)

    def writeFloat(self, value):
        data = struct.unpack(">f", value)
        assert len(data) == 4
        return self._stream.write(data)

    def writeDouble(self, value):
        data = struct.unpack(">d", value)
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

    def writeFully(self, bytes):
        self._buffer.append(bytes)
        self._count += len(bytes)

class DataOutputBuffer(DataOutputStream):
    def __init__(self):
        super(DataOutputStream, self).__init__(ByteArrayOutputStream())

    def getData(self):
        return self._stream.toByteArray()

    def getSize(self):
        return self._stream.size()

    def reset(self):
        self._stream.reset()

    def writeStreamData(self, input_stream, length):
        self._stream.write(input_stream.read(length))

