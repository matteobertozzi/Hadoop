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
from hadoop.io.SequenceFile import Metadata
from hadoop.io import LongWritable
from hadoop.io import SequenceFile

def writeData(writer):
    key = LongWritable()
    value = LongWritable()

    for i in xrange(10):
        key.set(1000 - i)
        value.set(i)
        print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        writer.append(key, value)

def testWrite(filename):
    metadata = Metadata()
    metadata.set('Meta Key 0', 'Meta Value 0')
    metadata.set('Meta Key 1', 'Meta Value 1')

    writer = SequenceFile.createWriter(filename, LongWritable, LongWritable, metadata)
    writeData(writer)
    writer.close()

def testRead(filename):
    reader = SequenceFile.Reader(filename)

    metadata = reader.getMetadata()
    for meta_key, meta_value in metadata:
        print 'METADATA:', meta_key, meta_value

    key_class = reader.getKeyClass()
    value_class = reader.getValueClass()

    key = key_class()
    value = value_class()

    position = reader.getPosition()
    while reader.next(key, value):
        print '*' if reader.syncSeen() else ' ',
        print '[%6s] %6s %6s' % (position, key.toString(), value.toString())
        position = reader.getPosition()

    reader.close()

if __name__ == '__main__':
    filename = 'test-meta.seq'
    testWrite(filename)
    testRead(filename)
