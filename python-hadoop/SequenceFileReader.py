#!/usr/bin/env python

import sys

from io import SequenceFile

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'usage: SequenceFileReader <filename>'
    else:
        reader = SequenceFile.Reader(sys.argv[1])

        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()

        key = key_class()
        value = value_class()

        #reader.sync(4042)
        position = reader.getPosition()
        while reader.next(key, value):
            print '*' if reader.syncSeen() else ' ',
            print '[%6s] %6s %6s' % (position, key.toString(), value.toString())
            position = reader.getPosition()

        reader.close()

