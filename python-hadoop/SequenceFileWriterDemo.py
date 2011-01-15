#!/usr/bin/env python

import sys

from io import SequenceFile, CompressionType, LongWritable

def writeData(writer):
    key = LongWritable()
    value = LongWritable()

    for i in xrange(1000):
        key.set(1000 - i)
        value.set(i)
        print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        writer.append(key, value)

if __name__ == '__main__':
    writer = SequenceFile.createWriter('test.seq', LongWritable, LongWritable)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-record.seq', LongWritable, LongWritable, compression_type=CompressionType.RECORD)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-block.seq', LongWritable, LongWritable, compression_type=CompressionType.BLOCK)
    writeData(writer)
    writer.close()

