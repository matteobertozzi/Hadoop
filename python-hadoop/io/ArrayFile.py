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

from IntWritable import LongWritable
import MapFile

class Writer(MapFile.Writer):
    def __init__(self, path, value_class):
        super(Writer, self).__init__(path, LongWritable, value_class)
        self._count = 0

    def append(self, value):
        super(Writer, self).append(LongWritable(self._count), value)
        self._count += 1

class Reader(MapFile.Reader):
    def __init__(self, path):
        super(Reader, self).__init__(path)
        self._key = LongWritable(0)

    def seek(self, n):
        if isinstance(n, LongWritable):
            n = n.get()

        self._key.set(n)
        return super(Reader, self).seek(self._key)

    def key(self):
        return self._key.get()

    def get(self, n, value):
        self._key.set(n)
        return(super(Reader, self).get(self._key, value))

