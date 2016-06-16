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

import lz4_raw
import struct
from cStringIO import StringIO

from hadoop.io.InputStream import DataInputBuffer

DEFAULT_BUF_SIZE = 256 * 1024
class Lz4Codec:
    def __init__(self):
        self.uncompressedBytes = 0
        self.blockSize = 0

    def compress(self, data):
        raise NotImplementedError

    def decompress(self, data):
        io = StringIO(data)
        if self.uncompressedBytes >= self.blockSize:
            self.blockSize = struct.unpack('>I', io.read(4))[0]
            self.uncompressedBytes = 0
            if self.blockSize == 0:
                return ""

	trunkSize = struct.unpack('>I', io.read(4))[0]
        f = lz4_raw.decompress(io.read(trunkSize), DEFAULT_BUF_SIZE)
        self.uncompressedBytes += len(f)
        return f

    def decompressInputStream(self, data):
        return DataInputBuffer(self.decompress(data))
