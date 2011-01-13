#!/usr/bin/env python

from Writable import AbstractValueWritable
from WritableUtils import readVInt, readVLong, writeVInt, writeVLong

class IntWritable(AbstractValueWritable):
    def write(self, data_output):
        data_output.writeInt(self._value)

    def readFields(self, data_input):
        self._value = data_input.readInt()

class LongWritable(AbstractValueWritable):
    def write(self, data_output):
        data_output.writeLong(self._value)

    def readFields(self, data_input):
        self._value = data_input.readLong()

class VIntWritable(AbstractValueWritable):
    def write(self, data_output):
        writeVInt(data_output, self._value)

    def readFields(self, data_input):
        self._value = readVInt(data_input)

class VLongWritable(AbstractValueWritable):
    def write(self, data_output):
        writeVLong(data_output, self._value)

    def readFields(self, data_input):
        self._value = readVLong(data_input)

