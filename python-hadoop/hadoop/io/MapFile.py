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

import os

from IntWritable import LongWritable
import SequenceFile

INDEX_FILE_NAME = 'index'
DATA_FILE_NAME = 'data'

class Writer(object):
    INDEX_INTERVAL = 128

    def __init__(self, dirname, key_class, value_class):
        os.mkdir(dirname)

        data_path = os.path.join(dirname, DATA_FILE_NAME)
        self._data = SequenceFile.createWriter(data_path, key_class, value_class)

        index_path = os.path.join(dirname, INDEX_FILE_NAME)
        self._index = SequenceFile.createBlockWriter(index_path, key_class, LongWritable)

        self._size = 0
        self._last_index_pos = -1
        self._last_index_nkeys = -4294967295

    def close(self):
        self._data.close()
        self._index.close()

    def append(self, key, value):
        self._checkKey(key)

        pos = self._data.getLength()
        if self._size >= self._last_index_nkeys + self.INDEX_INTERVAL and pos > self._last_index_pos:
            self._index.append(key, LongWritable(pos))
            self._last_index_pos = pos
            self._last_index_nkeys = self._size

        self._data.append(key, value)
        self._size += 1

    def _checkKey(self, key):
        pass

class Reader(object):
    INDEX_SKIP = 0

    def __init__(self, dirname):
        self._data = SequenceFile.Reader(os.path.join(dirname, DATA_FILE_NAME))
        self._index = SequenceFile.Reader(os.path.join(dirname, INDEX_FILE_NAME))
        self._first_position = self._data.getPosition()
        self._positions = []
        self._keys = []

    def close(self):
        self._data.close()
        self._index.close()

    def getIndexInterval(self):
        return self._index_interval

    def setIndexInterval(self, interval):
        self._index_interval = interval

    def reset(self):
        self._data.seek(self._first_position)

    def midKey(self):
        self._readIndex()
        count = len(self._keys)
        if count == 0:
            return None
        return self._keys[(count - 1) >> 1]

    def finalKey(self, key):
        original_position = self._data.getPosition()
        try:
            self._readIndex()
            count = len(self._keys)
            if count > 0:
                self._data.seek(self._positions[count - 1])
            else:
                self._reset()
            while self._data.nextKey(key):
                continue
        finally:
            self._data.seek(original_position)

    def seek(self, key):
        return self._seekInternal(key) == 0

    def next(self, key, value):
        return self._data.next(key, value)

    def get(self, key, value):
        if self.seek(key):
            self._data._getCurrentValue(value)
            return value
        return None

    def getClosest(self, key, value, before=False):
        c = self._seekInternal(key, before)
        if (not before and c > 0) or (before and c < 0):
            return None

        self._data._getCurrentValue(value)
        return self._next_key

    def _readIndex(self):
        if self._keys:
            return

        key_class = self._index.getKeyClass()

        skip = self.INDEX_SKIP
        position = LongWritable()
        last_position = None
        while True:
            key = key_class()
            if not self._index.next(key, position):
                break

            if skip > 0:
                skip -= 1
                continue

            skip = self.INDEX_SKIP
            if position.get() == last_position:
                continue

            self._positions.append(position.get())
            self._keys.append(key)

    def _seekInternal(self, key, before=None):
        self._readIndex()

        seek_index = self._indexSearch(key)
        if seek_index < 0:
            seek_index = -seek_index - 2

        if seek_index == -1:
            seek_position = self._first_position
        else:
            seek_position = self._positions[seek_index]

        prev_position = -1
        curr_position = seek_position

        key_class = self._data.getKeyClass()
        self._next_key = key_class()

        self._data.seek(seek_position)
        while self._data.nextKey(self._next_key):
            cmp = key.compareTo(self._next_key)
            if cmp <= 0:
                if before and cmp != 0:
                    if prev_position == -1:
                        self._data.seek(curr_position)
                    else:
                        self._data.seek(prev_position)
                        self._data.nextKey(self._next_key)
                        return 1
                return cmp

            if before:
                prev_position = curr_position
                curr_position = self._data.getPosition()

        return 1

    def _indexSearch(self, key):
        high = len(self._keys) - 1
        low = 0

        while low <= high:
            mid = (low + high) >> 1

            cmp = self._keys[mid].compareTo(key)
            if cmp < 0:
                low = mid + 1
            elif cmp > 0:
                high = mid - 1
            else:
                return mid
        return -(low + 1)

