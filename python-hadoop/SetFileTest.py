#!/usr/bin/env python

from io.IntWritable import IntWritable
from io import SetFile

if __name__ == '__main__':
    writer = SetFile.Writer('set-test', IntWritable)
    writer.INDEX_INTERVAL = 16
    for i in xrange(0, 100, 2):
        writer.append(IntWritable(i * 10))
    writer.close()

    key = IntWritable()
    reader = SetFile.Reader('set-test')
    while reader.next(key):
        print key

    print 'GET 8'
    key.set(8)
    print reader.get(key)
    print

    print 'GET 120'
    key.set(120)
    print reader.get(key)
    print

    print 'GET 240'
    key.set(240)
    print reader.get(key)
    print

    print 'GET 550'
    key.set(550)
    print reader.get(key)
    print

    reader.close()
