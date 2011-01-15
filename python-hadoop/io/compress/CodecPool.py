#!/usr/bin/env python

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

