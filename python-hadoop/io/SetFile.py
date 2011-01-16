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

from NullWritable import NullWritable
import MapFile

class Writer(MapFile.Writer):
    def __init__(self, path, key_class):
        super(Writer, self).__init__(path, key_class, NullWritable)

    def append(self, key):
        return super(Writer, self).append(key, NullWritable())

class Reader(MapFile.Reader):
    def next(self, key):
        return super(Reader, self).next(key, NullWritable())

    def get(self, key):
        if self.seek(key):
            return self._next_key
        return None

