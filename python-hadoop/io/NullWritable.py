#!/usr/bin/env python

from Writable import WritableComparable

class NullWritable(WritableComparable):
    def __new__(cls, *p, **k):
        if not '_shared_instance' in cls.__dict__:
            cls._shared_instance = WritableComparable.__new__(cls)
        return cls._shared_instance

    def write(self, data_output):
        pass

    def readFields(self, data_input):
        pass

    def toString(self):
        return "(null)"

    def hashCode(self):
        return 0

    def equals(self, other):
        return isinstance(other, NullWritable)

    def compareTo(self, other):
        assert isinstance(other, NullWritable)
        assert self is other
        return self is other # True

