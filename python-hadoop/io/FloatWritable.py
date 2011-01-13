#!/usr/bin/env python

from Writable import AbstractValueWritable

class FloatWritable(AbstractValueWritable):
    def write(self, data_output):
        data_output.writeFloat(self._value)

    def readFields(self, data_input):
        self._value = data_input.readFloat()

class DoubleWritable(AbstractValueWritable):
    def write(self, data_output):
        data_output.writeDouble(self._value)

    def readFields(self, data_input):
        self._value = data_input.readDouble()

