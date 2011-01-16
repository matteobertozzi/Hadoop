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

