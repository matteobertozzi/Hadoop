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

from hadoop.io.SequenceFile import CompressionType
from hadoop.io import LongWritable
from hadoop.io import SequenceFile

def writeData(writer):
    key = LongWritable()
    value = LongWritable()

    for i in xrange(1000):
        key.set(1000 - i)
        value.set(i)
        print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        writer.append(key, value)

if __name__ == '__main__':
    writer = SequenceFile.createWriter('test.seq', LongWritable, LongWritable)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-record.seq', LongWritable, LongWritable, compression_type=CompressionType.RECORD)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-block.seq', LongWritable, LongWritable, compression_type=CompressionType.BLOCK)
    writeData(writer)
    writer.close()

