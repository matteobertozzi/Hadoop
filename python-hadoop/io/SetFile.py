#!/usr/bin/env python

from NullWritable import NullWritable
import MapFile

class Writer(MapFile.Writer):
    def __init__(self, path, key_class):
        super(Writer, self).__init__(path, key_class, NullWritable)

    def append(self, key):
        return super(Writer, self).append(key, NullWritable())

class Reader(MapFile.Reader):
    def next(self, key):
        return super(Reader, self).next(key, NullWritable())

    def get(self, key):
        if self.seek(key):
            return self._next_key
        return None

