#!/bin/bash

HADOOP_DEV_DIR="$(cd "$(dirname "$0")" && pwd)"
HADOOP_DEV_LOGS="$HADOOP_DEV_DIR/logs/"

. $HADOOP_DEV_DIR/hadoop-sources.sh

hadoop_start() {
    echo "Hadoop is starting..."

    rm -rf /tmp/*
    rm -rf $HADOOP_DEV_LOGS
    mkdir -p $HADOOP_DEV_LOGS

    echo " - HDFS Namenode format"
    hdfs namenode -format > /dev/null

    echo " - HDFS Namenode start"
    hdfs namenode &> $HADOOP_DEV_LOGS/hdfs-namenode.log &

    echo " - HDFS Datanode start"
    hdfs datanode &> $HADOOP_DEV_LOGS/hdfs-datanode.log &

    echo " - YARN Resource Manager start"
    yarn resourcemanager &> $HADOOP_DEV_LOGS/yarn-resourcemanager.log &

    echo " - YARN Node Manager start"
    yarn nodemanager &> $HADOOP_DEV_LOGS/yarn-nodemanager.log &
}

hadoop_stop() {
    echo "Hadoop is stopping..."
    killall java
}

if [ $# -lt 1 ]; then
    echo "usage: hadoop-starter <start|stop>"
    exit 1
elif [ "$1" = "start" ]; then
    hadoop_start
elif [ "$1" = "stop" ]; then
    hadoop_stop
else
    echo "$1 Invalid option..."
fi

