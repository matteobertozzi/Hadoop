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

from hadoop.io.IntWritable import LongWritable, IntWritable
from hadoop.io import ArrayFile

if __name__ == '__main__':
    writer = ArrayFile.Writer('array-test', IntWritable)
    writer.INDEX_INTERVAL = 16
    for i in xrange(0, 100):
        writer.append(IntWritable(1 + i * 10))
    writer.close()

    key = LongWritable()
    value = IntWritable()
    reader = ArrayFile.Reader('array-test')
    while reader.next(key, value):
        print key, value

    print 'GET 8'
    print reader.get(8, value)
    print value
    print

    print 'GET 110'
    print reader.get(110, value)
    print

    print 'GET 25'
    print reader.get(25, value)
    print value
    print

    print 'GET 55'
    print reader.get(55, value)
    print value
    print

    reader.close()
