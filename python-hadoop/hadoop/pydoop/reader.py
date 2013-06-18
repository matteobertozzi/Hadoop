"""pure-python implementation of pydoop-capable and sequencefile-capable
record reader.

Inclue pydoop as a dependency for this package by instructing pip or
setuptools that the `[pydoop]` extra is required:

    pip install Hadoop[pydoop]

Depends on recent implementations of pydoop, which support
custom python recordreaders.

TODO: profile this, and the rest of hadoop.io! cython speedups,
perhaps?

"""
import logging
logger = logging.getLogger(__name__)
try:
    import pydoop.pipes as pp
    from pydoop import hdfs
except ImportError as e:
    raise Exception("cannot load pydoop " +
                    "(not installed as Hadoop[pydoop]?): %s" % e)
from hadoop.io import SequenceFile, InputStream


class HdfsFileInputStream(InputStream.FileInputStream):
    """meets hadoop interface, at least all the bits that
    FileInputStream implements"""
    def __init__(self, path):
        logger.debug("FileInputStream path: %s", path)
        self._fd = hdfs.open(path, 'r') # todo: get user
        self._length = self._fd.size


class _HdfsSequenceFileReader(SequenceFile.Reader):
    def getStream(self, path):
        logger.debug("_HdfsSequenceFileReader path: %s", path)
        return InputStream.DataInputStream(HdfsFileInputStream(path))


class SequenceFileReader(pp.RecordReader):
    """custom python record reader that reads Java-style sequence
    files from HDFS. Caveat is that objects in the sequence file must
    be declared and in scope with the same namespace as Java.

    The Hadoop package (on which this depends) provides classes for
    the types org.apache.hadoop.io.Text etc, but other classes
    (e.g. com.intelius.avroidm.IDMArray) must be provided, and must
    meet the interfaces expected by the Hadoop package (see the Text
    implementation there)."""
    def __init__(self, context):
        super(SequenceFileReader, self).__init__()
        self.isplit = pp.InputSplit(context.getInputSplit())
        logger.debug("isplit filename: %s", self.isplit.filename)
        logger.debug("isplit offset: %s", self.isplit.offset)
        logger.debug("isplit length: %s", self.isplit.length)
        self.seq_file = _HdfsSequenceFileReader(path = self.isplit.filename,
                                                start = self.isplit.offset,
                                                length = self.isplit.length)

        key_class = self.seq_file.getKeyClass()
        value_class = self.seq_file.getValueClass()
        self._key = key_class()
        self._value = value_class()
        logger.debug("done initializing pydoop.reader.SequenceFileReader")

    def close(self):
        self.seq_file.close()

    def next(self):
        if (self.seq_file.next(self._key, self._value)):
            return (True, self._key.toString(), self._value.toString())
        else:
            return (False, "", "")

    def getProgress(self):
        result = float(self.seq_file.getPosition())/self.isplit.length
        return min(result, 1.0)
