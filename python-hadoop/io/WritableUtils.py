#!/usr/bin/env python

def readVInt(data_input):
    return readVLong(data_input)

def readVLong(data_input):
    first_byte = data_input.readByte()
    length = decodeVIntSize(first_byte)
    if length == 1:
        return first_byte

    i = 0
    for idx in xrange(length - 1):
        b = data_input.readByte()
        if b < 0:
            b = -b | 0x80

        i = i << 8
        i = i | (b & 0xFF)

    return (i ^ -1) if isNegativeVInt(first_byte) else i

def writeVInt(data_output, value):
    return writeVLong(data_output, value)

def writeVLong(data_output, value):
    if value >= -112 and value <= 127:
        data_output.writeByte(value)
        return

    length = -112
    if value < 0:
        value ^= -1
        length -= 120

    temp = value
    while temp != 0:
        temp = temp >> 8
        length -= 1

    data_output.writeByte(length)
    length = -(length + 120) if (length < -120) else -(length + 112)
    for idx in reversed(range(length)):
        shiftbits = idx << 3
        mask = 0xFF << shiftbits

        x = (value & mask) >> shiftbits
        data_output.writeByte(-(x & 0x7f) if x & 0x80 else x)

def isNegativeVInt(value):
    return value < -120 or (value >= -112 and value < 0)

def decodeVIntSize(value):
    if value >= -112:
        return 1
    elif value < -120:
        return -119 - value
    return -111 - value
