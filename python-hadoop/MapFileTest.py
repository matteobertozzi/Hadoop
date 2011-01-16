#!/usr/bin/env python

from io.IntWritable import LongWritable
from io import MapFile

if __name__ == '__main__':
    writer = MapFile.Writer('map-test', LongWritable, LongWritable)
    writer.INDEX_INTERVAL = 2
    for i in xrange(0, 100, 2):
        writer.append(LongWritable(i), LongWritable(i * 10))
    writer.close()

    key = LongWritable()
    value = LongWritable()
    reader = MapFile.Reader('map-test')
    while reader.next(key, value):
        print key, value

    print 'GET CLOSEST'
    key.set(8)
    print reader.get(key, value)
    print value
    print

    print 'GET 111'
    key.set(111)
    print reader.get(key, value)
    print

    key.set(25)
    print 'SEEK 25 before'
    print reader.getClosest(key, value, before=True)
    print value
    print

    key.set(55)
    print 'SEEK 55'
    print reader.getClosest(key, value)
    print value
    print

    reader.close()
