#!/bin/sh

HADOOP_USER="hadoop"

HADOOP_SRC="/usr/local/src/hadoop"
HADOOP_SRC_COMMON="$HADOOP_SRC/hadoop-common"
HADOOP_SRC_HDFS="$HADOOP_SRC/hadoop-hdfs"
HADOOP_SRC_MAPREDUCE="$HADOOP_SRC/hadoop-mapreduce"

HADOOP_BUILDS="/usr/local/hadoop-builds"
HADOOP_BUILDNR="$HADOOP_BUILDS/.buildnr"

HADOOP_HOME="/usr/local/hadoop"
HADOOP_CONF="/root/hadoop-conf"

hadoop_build() {
    echo "Build Hadoop Common"
    cd $HADOOP_SRC_COMMON
    ant jar > /dev/null

    echo "Build Hadoop HDFS"
    cd $HADOOP_SRC_HDFS
    ant jar > /dev/null

    echo "Build Hadoop Map-Reduce"
    cd $HADOOP_SRC_MAPREDUCE
    ant jar > /dev/null
}

hadoop_install() {
    mkdir -p $HADOOP_BUILDS
    touch $HADOOP_BUILDNR
    build_number=$((`cat $HADOOP_BUILDNR` + 1))
    echo $build_number > $HADOOP_BUILDNR

    echo "Hadoop Install $build_number"
    hadoop_build="$HADOOP_BUILDS/hadoop-$build_number"
    mkdir -p $hadoop_build
    rm -f $HADOOP_HOME
    ln -s $hadoop_build $HADOOP_HOME

    ln -s /etc/hadoop $HADOOP_HOME/conf
    mkdir -p $HADOOP_HOME/classes
    mkdir -p $HADOOP_HOME/bin
    mkdir -p $HADOOP_HOME/lib

    cp -r $HADOOP_SRC_COMMON/build/hadoop-common-*.jar $HADOOP_HOME/
    cp -r $HADOOP_SRC_HDFS/build/hadoop-hdfs-*.jar $HADOOP_HOME/
    cp -r $HADOOP_SRC_MAPREDUCE/build/hadoop-mapred-*.jar $HADOOP_HOME/

    cp -r $HADOOP_SRC_COMMON/bin/* $HADOOP_HOME/bin/
    cp -r $HADOOP_SRC_HDFS/bin/* $HADOOP_HOME/bin/
    cp -r $HADOOP_SRC_MAPREDUCE/bin/* $HADOOP_HOME/bin/

    cp -r $HADOOP_SRC_COMMON/build/classes/* $HADOOP_HOME/classes/
    cp -r $HADOOP_SRC_HDFS/build/classes/* $HADOOP_HOME/classes/
    cp -r $HADOOP_SRC_MAPREDUCE/build/classes/* $HADOOP_HOME/classes/

    cp $HADOOP_SRC_MAPREDUCE/build/ivy/lib/Hadoop/common/*.jar $HADOOP_HOME/lib/
    rm -f $HADOOP_HOME/lib/hadoop-common-*.jar
    rm -f $HADOOP_HOME/lib/hadoop-hdfs-*.jar

    echo "$hadoop_build"
    echo $HADOOP_HOME

    chown -R $HADOOP_USER:$HADOOP_USER $HADOOP_HOME
    chown -R $HADOOP_USER:$HADOOP_USER $HADOOP_BUILDS

    echo "Hadoop new conf files $HADOOP_CONF"
    rm -rf $HADOOP_CONF
    mkdir -p $HADOOP_CONF
    cp -r $HADOOP_SRC_COMMON/conf/* $HADOOP_CONF/
    cp -r $HADOOP_SRC_HDFS/conf/* $HADOOP_CONF/
    cp -r $HADOOP_SRC_MAPREDUCE/conf/* $HADOOP_CONF/
}

hadoop_build
hadoop_install

