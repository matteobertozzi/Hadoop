#!/usr/bin/env python

from Writable import WritableComparable
from WritableUtils import readVInt, writeVInt

class Text(WritableComparable):
    def __init__(self):
        self._bytes = ''
        self._length = 0

    def write(self, data_output):
        writeVInt(data_output, self._length)
        data_output.write(self._bytes, 0, self._length)

    def readFields(self, data_input):
        self._length = readVInt(data_input)
        self._bytes = data_input.read(self._length)

    def equal(self, other):
        if not isinstance(other, Text):
            return False
        return self._bytes == other._bytes and self._length and other._length

    def toString(self):
        return self._bytes

    @staticmethod
    def readString(data_input):
        length = readVInt(data_input)
        bytes = data_input.read(length)
        return Text.decode(bytes)

    @staticmethod
    def decode(bytes):
        return bytes

