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

from util import ReflectionUtils

from BZip2Codec import *
from ZlibCodec import *

class CodecPool(object):
    def __new__(cls, *p, **k):
        if not '_shared_instance' in cls.__dict__:
            cls._shared_instance = object.__new__(cls)
        return cls._shared_instance

    def getDecompressor(self, class_path=None):
        if not class_path:
            return DefaultCodec()
        codec_class = ReflectionUtils.hadoopClassFromName(class_path)
        return codec_class()

    def getCompressor(self, class_path=None):
        if not class_path:
            return DefaultCodec()
        codec_class = ReflectionUtils.hadoopClassFromName(class_path)
        return codec_class()

