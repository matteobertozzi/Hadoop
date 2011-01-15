#!/usr/bin/env python

import bz2

from io.InputStream import DataInputBuffer

class BZip2Codec:
    def compress(self, data):
        return bz2.compress(data)

    def decompress(self, data):
        return bz2.decompress(data)

    def decompressInputStream(self, data):
        return DataInputBuffer(bz2.decompress(data))

