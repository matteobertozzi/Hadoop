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

from hadoop.io.IntWritable import IntWritable
from hadoop.io import SetFile

if __name__ == '__main__':
    writer = SetFile.Writer('set-test', IntWritable)
    writer.INDEX_INTERVAL = 16
    for i in xrange(0, 100, 2):
        writer.append(IntWritable(i * 10))
    writer.close()

    key = IntWritable()
    reader = SetFile.Reader('set-test')
    while reader.next(key):
        print key

    print 'GET 8'
    key.set(8)
    print reader.get(key)
    print

    print 'GET 120'
    key.set(120)
    print reader.get(key)
    print

    print 'GET 240'
    key.set(240)
    print reader.get(key)
    print

    print 'GET 550'
    key.set(550)
    print reader.get(key)
    print

    reader.close()
