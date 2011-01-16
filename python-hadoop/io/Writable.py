#!/usr/bin/env python

class Writable(object):
    def write(self, data_output):
        raise NotImplementedError

    def readFields(self, data_input):
        raise NotImplementedError

    def toString(self):
        return str(type(self))

    def __repr__(self):
        return self.toString()

class WritableComparable(Writable):
    def compareTo(self, other):
        raise NotImplementedError

class AbstractValueWritable(WritableComparable):
    def __init__(self, value=None):
        assert not isinstance(value, type(self)), (type(self._value))
        self._value = value

    def set(self, value):
        assert not isinstance(self._value, type(self)), (type(self._value))
        self._value = value

    def get(self):
        return self._value

    def equal(self, other):
        if not isinstance(other, type(self)):
            return False
        return self._value == other._value

    def compareTo(self, other):
        assert isinstance(other, type(self)), (type(self), type(other))
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
        assert not isinstance(self._value, type(self)), (type(self._value))
        return str(self._value)

