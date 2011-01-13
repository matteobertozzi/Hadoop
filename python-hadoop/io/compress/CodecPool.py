#!/usr/bin/env python

from ZlibCodec import *

class CodecPool(object):
    def __new__(cls, *p, **k):
        if not '_shared_instance' in cls.__dict__:
            cls._shared_instance = object.__new__(cls)

        shared_instance = cls._shared_instance

        shared_instance._decompressors = { None: ZlibDecompressor,
                                           'org.apache.hadoop.io.compress.DefaultCodec': ZlibDecompressor,
                                          }
        shared_instance._compressors = { None: ZlibDecompressor,
                                        'org.apache.hadoop.io.compress.DefaultCodec': ZlibDecompressor,
                                       }
        return shared_instance

    def getDecompressor(self, class_path=None):
        decompressor_class = self._decompressors[class_path]
        return decompressor_class()

    def getCompressor(self, class_path=None):
        compressor_class = self._compressors[class_path]
        return compressor_class()

