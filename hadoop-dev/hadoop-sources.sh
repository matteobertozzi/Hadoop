#!/bin/bash

export HADOOP_SRC="/home/hadoop/hadoop-src/hadoop-trunk"
export HBASE_SRC="/home/hadoop/hadoop-src/hbase-trunk"
export JAVA_HOME="/usr/lib/jvm/java-6-sun"

export HADOOP_COMMON_HOME="$(ls -d $HADOOP_SRC/hadoop-common-project/hadoop-common/target/hadoop-common-*-SNAPSHOT)"
export HADOOP_HDFS_HOME="$(ls -d $HADOOP_SRC/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-*-SNAPSHOT)"
export HADOOP_MAPRED_HOME="$(ls -d $HADOOP_SRC/hadoop-mapreduce-project/target/hadoop-mapreduce-*-SNAPSHOT)"
export HBASE_HOME="$(ls -d $HBASE_SRC/target/hbase-*-SNAPSHOT/hbase-*-SNAPSHOT)"
export YARN_HOME="$HADOOP_MAPRED_HOME"
export PATH="$HBASE_HOME/bin:$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH"

