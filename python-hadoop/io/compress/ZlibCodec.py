#!/usr/bin/env python

import zlib

from io.InputStream import DataInputBuffer

class ZlibCompressor(object):
    pass

class ZlibDecompressor(object):
    def __init__(self):
        self._data = 0
        self._offset = 0
        self._length = 0

    def setInput(self, data, offset=0, length=0):
        if data and not length:
            length = len(data) - offset

        self._data = data
        self._offset = offset
        self._length = length

    def decompress(self):
        return zlib.decompress(self._data[self._offset:self._offset+self._length])

    def inputStream(self):
        return DataInputBuffer(self.decompress())

