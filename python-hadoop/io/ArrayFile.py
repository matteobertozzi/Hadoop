#!/usr/bin/env python

from IntWritable import LongWritable
import MapFile

class Writer(MapFile.Writer):
    def __init__(self, path, value_class):
        super(Writer, self).__init__(path, LongWritable, value_class)
        self._count = 0

    def append(self, value):
        super(Writer, self).append(LongWritable(self._count), value)
        self._count += 1

class Reader(MapFile.Reader):
    def __init__(self, path):
        super(Reader, self).__init__(path)
        self._key = LongWritable(0)

    def seek(self, n):
        if isinstance(n, LongWritable):
            n = n.get()

        self._key.set(n)
        return super(Reader, self).seek(self._key)

    def key(self):
        return self._key.get()

    def get(self, n, value):
        self._key.set(n)
        return(super(Reader, self).get(self._key, value))

