#!/usr/bin/env python
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from Writable import WritableComparable
from WritableUtils import readVInt, writeVInt

class Text(WritableComparable):
    def __init__(self):
        self._bytes = ''
        self._length = 0

    def getBytes(self):
        return self._bytes

    def getLength(self):
        return self._length

    def set(self, value):
        self._bytes = Text.encode(value)
        self._length = len(self._bytes)

    def append(self, value):
        new_bytes = Text.encode(value)
        self._bytes += new_bytes
        self._length += len(new_bytes)

    def clear(self):
        self._length = 0
        self._bytes = ''

    def write(self, data_output):
        writeVInt(data_output, self._length)
        data_output.write(self._bytes)

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
    def writeString(data_output, bytes):
        bytes = Text.encode(bytes)
        writeVInt(data_output, len(bytes))
        data_output.write(bytes)

    @staticmethod
    def encode(bytes):
        return bytes.encode('utf-8')

    @staticmethod
    def decode(bytes):
        return bytes.decode('utf-8')

