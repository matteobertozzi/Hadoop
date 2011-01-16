#!/usr/bin/env python

from io.IntWritable import LongWritable, IntWritable
from io import ArrayFile

if __name__ == '__main__':
    writer = ArrayFile.Writer('array-test', IntWritable)
    writer.INDEX_INTERVAL = 16
    for i in xrange(0, 100):
        writer.append(IntWritable(1 + i * 10))
    writer.close()

    key = LongWritable()
    value = IntWritable()
    reader = ArrayFile.Reader('array-test')
    while reader.next(key, value):
        print key, value

    print 'GET 8'
    print reader.get(8, value)
    print value
    print

    print 'GET 110'
    print reader.get(110, value)
    print

    print 'GET 25'
    print reader.get(25, value)
    print value
    print

    print 'GET 55'
    print reader.get(55, value)
    print value
    print

    reader.close()
