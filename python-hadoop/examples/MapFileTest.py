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

from hadoop.io.IntWritable import LongWritable
from hadoop.io import MapFile

if __name__ == '__main__':
    writer = MapFile.Writer('map-test', LongWritable, LongWritable)
    writer.INDEX_INTERVAL = 2
    for i in xrange(0, 100, 2):
        writer.append(LongWritable(i), LongWritable(i * 10))
    writer.close()

    key = LongWritable()
    value = LongWritable()
    reader = MapFile.Reader('map-test')
    while reader.next(key, value):
        print key, value

    print 'MID KEY', reader.midKey()
    print 'FINAL KEY', reader.finalKey(key), key

    print 'GET CLOSEST'
    key.set(8)
    print reader.get(key, value)
    print value
    print

    print 'GET 111'
    key.set(111)
    print reader.get(key, value)
    print

    key.set(25)
    print 'SEEK 25 before'
    print reader.getClosest(key, value, before=True)
    print value
    print

    key.set(55)
    print 'SEEK 55'
    print reader.getClosest(key, value)
    print value
    print

    reader.close()
