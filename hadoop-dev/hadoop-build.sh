#!/bin/bash

HADOOP_DEV_DIR="$(cd "$(dirname "$0")" && pwd)"
HADOOP_DEV_CONF="$HADOOP_DEV_DIR/pseudo-conf"

HADOOP_SRC="/home/hadoop/hadoop-src/hadoop-trunk"
HBASE_SRC="/home/hadoop/hadoop-src/hbase-trunk"

hadoop_build() {
    cd $HADOOP_SRC
    mvn clean
    mvn package -Pdist -Pnative -P-cbuild -DskipTests -DskipJavadoc

    HADOOP_DIST="$(ls -d $HADOOP_SRC/hadoop-dist/target/hadoop-*-SNAPSHOT)"
    HADOOP_COMMON_HOME="$HADOOP_DIST"
    HADOOP_HDFS_HOME="$HADOOP_DIST"
    HADOOP_MAPRED_HOME="$HADOOP_DIST"
    HADOOP_CONF="$HADOOP_DIST/etc/hadoop"

    cp $HADOOP_DEV_CONF/core-site.xml $HADOOP_CONF/core-site.xml
    cp $HADOOP_DEV_CONF/hdfs-site.xml $HADOOP_CONF/hdfs-site.xml
    cp $HADOOP_DEV_CONF/yarn-site.xml $HADOOP_CONF/yarn-site.xml
}

hbase_build() {
    cd $HBASE_SRC
    mvn clean
    mvn -P-cbuild -DskipTests package

    HBASE_HOME="$(ls -d $HBASE_SRC/target/hbase-*-SNAPSHOT/hbase-*-SNAPSHOT)"
    cp $HADOOP_DEV_DIR/pseudo-conf/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml
}

if [ $# -lt 1 ]; then
    hadoop_build
    hbase_build
elif [ "$1" = "hadoop" ]; then
    hadoop_build
elif [ "$1" = "hbase" ]; then
    hbase_build
else
    echo "$1 Invalid option..."
    echo "usage: hadoo-build [hadoop|hbase]"
fi

