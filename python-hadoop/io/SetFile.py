#!/usr/bin/env python

import NullWritable
import MapFile

class Writer(MapFile.Writer):
    def append(self, key):
        return super(Writer, self).append(key, NullWritable())

class Reader(MapFile.Reader):
    def next(self, key):
        return super(Reader, self).next(key, NullWritable())

    def get(self, key):
        if self.seek(key):
            self.next(key)
            return key
        return None

