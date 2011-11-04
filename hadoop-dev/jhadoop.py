#!/usr/bin/env python
# Helper to compile & execute Single file java demo.
# Usage:
#      compile with -> jhadoop TestFile.java
#      execute with -> jhadoop TestFile.class
# Remember to export:
#   HADOOP_COMMON_HOME
#   HADOOP_HDFS_HOME
#   HADOOP_MAPRED_HOME
#   HBASE_HOME

from commands import getstatusoutput as execCommand
import mimetypes
import sys
import os

def hadoopClassPath():
    def _findJars(paths):
        jars = {'.':'.'}
        for path in paths:
            if path is None:
                continue

            for root, dirs, files in os.walk(path, topdown=False):
                for name in files:
                    if name.endswith('.jar'):
                        jars[name] = os.path.join(root, name)
        return jars.values()

    hadoop_env = ('HADOOP_COMMON_HOME',
                  'HADOOP_HDFS_HOME',
                  'HADOOP_MAPRED_HOME',
                  'HBASE_HOME',
                 )

    return ':'.join(_findJars(os.getenv(henv) for henv in reversed(hadoop_env)))

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print 'usage:'
        print '     jhadoop FileName.java'
        print '     jhadoop FileName.class'
        sys.exit(1)

    mime, _ = mimetypes.guess_type(sys.argv[1])
    if mime == 'text/x-java':
        sources = ' '.join(sys.argv[1:])
        cmd = 'javac -classpath %s %s' % (hadoopClassPath(), sources)
    elif mime == 'application/java-vm':
        cmd = 'java -classpath %s %s' % (hadoopClassPath(), sys.argv[1][:-6])
    else:
        raise TypeError(mime)

    exit_code, output = execCommand(cmd)
    print output

