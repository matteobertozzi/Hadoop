#!/usr/bin/env python

import zlib

from io.InputStream import DataInputBuffer

class ZlibCodec:
    def compress(self, data):
        return zlib.compress(data)

    def decompress(self, data):
        return zlib.decompress(data)

    def decompressInputStream(self, data):
        return DataInputBuffer(zlib.decompress(data))

DefaultCodec = ZlibCodec

