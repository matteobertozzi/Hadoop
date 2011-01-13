#!/usr/bin/env python

class Writable(object):
    def write(self, data_output):
        raise NotImplementedError

    def readFields(self, data_input):
        raise NotImplementedError

class WritableComparable(Writable):
    def compareTo(self, other):
        raise NotImplementedError

class AbstractValueWritable(WritableComparable):
    def __init__(self, value=None):
        self._value = value

    def set(self, value):
        self._value = value

    def get(self, value):
        return self._value

    def equal(self, other):
        if not isinstance(other, type(self)):
            return False
        return self._value == other._value

    def compareTo(self, other):
        assert isinstance(other, type(self))
        a = self._value
        b = other._value
        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    def hashCode(self):
        return int(self._value)

    def toString(self):
        return str(self._value)

