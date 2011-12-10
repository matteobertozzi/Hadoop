#!/bin/bash

export HADOOP_SRC="/home/hadoop/hadoop-src/hadoop-trunk"
export HBASE_SRC="/home/hadoop/hadoop-src/hbase-trunk"
export JAVA_HOME="/usr/lib/jvm/java-6-sun"

HADOOP_DIST="$(ls -d $HADOOP_SRC/hadoop-dist/target/hadoop-*-SNAPSHOT)"
export HADOOP_COMMON_HOME="$HADOOP_DIST"
export HADOOP_HDFS_HOME="$HADOOP_DIST"
export HADOOP_MAPRED_HOME="$HADOOP_DIST"
export HBASE_HOME="$(ls -d $HBASE_SRC/target/hbase-*-SNAPSHOT/hbase-*-SNAPSHOT)"
export YARN_HOME="$HADOOP_MAPRED_HOME"
export PATH="$HBASE_HOME/bin:$HADOOP_DIST/bin:$PATH"

