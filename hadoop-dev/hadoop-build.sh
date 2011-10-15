#!/bin/bash

HADOOP_DEV_DIR="$(cd "$(dirname "$0")" && pwd)"
HADOOP_SRC="/home/hadoop/hadoop-src/hadoop-trunk"

cd $HADOOP_SRC
mvn clean package -Pdist -Pnative -P-cbuild -DskipTests

HADOOP_COMMON_HOME="$(ls -d $HADOOP_SRC/hadoop-common-project/hadoop-common/target/hadoop-common-*-SNAPSHOT)"
HADOOP_HDFS_HOME="$(ls -d $HADOOP_SRC/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-*-SNAPSHOT)"
HADOOP_MAPRED_HOME="$(ls -d $HADOOP_SRC/hadoop-mapreduce-project/target/hadoop-mapreduce-*-SNAPSHOT)"

cp $HADOOP_DEV_DIR/pseudo-conf/core-site.xml $HADOOP_COMMON_HOME/etc/hadoop/core-site.xml
cp $HADOOP_DEV_DIR/pseudo-conf/yarn-site.xml $HADOOP_MAPRED_HOME/conf/yarn-site.xml


cd $HBASE_SRC
mvn -P-cbuild -DskipTests package

HBASE_HOME="$(ls -d $HBASE_SRC/target/hbase-*-SNAPSHOT/hbase-*-SNAPSHOT)"
cp $HADOOP_DEV_DIR/pseudo-conf/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

