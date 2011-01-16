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

