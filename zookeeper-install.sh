#!/bin/bash

HADOOP_USER="hadoop"

HADOOP_SRC="/usr/local/src/hadoop"

ZOOKEEPER_SRC="$HADOOP_SRC/zookeeper"
ZOOKEEPER_BUILDS="/usr/local/zookeeper-builds"
ZOOKEEPER_BUILDNR="$ZOOKEEPER_BUILDS/.buildnr"
ZOOKEEPER_HOME="/usr/local/zookeeper"
ZOOKEEPER_CONF="/root/zookeeper-conf"

zookeeper_build() {
    echo "Build Zookeeper"
    cd $ZOOKEEPER_SRC
    ant jar > /dev/null
}

zookeeper_install() {
    mkdir -p $ZOOKEEPER_BUILDS
    touch $ZOOKEEPER_BUILDNR
    build_number=$((`cat $ZOOKEEPER_BUILDNR` + 1))
    echo $build_number > $ZOOKEEPER_BUILDNR

    echo "Zookeeper Install $build_number"
    zookeeper_build="$ZOOKEEPER_BUILDS/zookeeper-$build_number"
    mkdir -p $zookeeper_build
    rm -f $ZOOKEEPER_HOME
    ln -s $zookeeper_build $ZOOKEEPER_HOME

    ln -s /etc/zookeeper $ZOOKEEPER_HOME/conf
    mkdir -p $ZOOKEEPER_HOME/classes
    mkdir -p $ZOOKEEPER_HOME/bin
    mkdir -p $ZOOKEEPER_HOME/lib

    cp -r $ZOOKEEPER_SRC/build/zookeeper-*.jar $ZOOKEEPER_HOME/

    cp -r $ZOOKEEPER_SRC/bin/* $ZOOKEEPER_HOME/bin/

    cp -r $ZOOKEEPER_SRC/build/classes/* $ZOOKEEPER_HOME/classes/

    cp $ZOOKEEPER_SRC/build/lib/*.jar $ZOOKEEPER_HOME/lib/

    echo "$zookeeper_build"
    echo $ZOOKEEPER_HOME

    chown -R $HADOOP_USER:$HADOOP_USER $ZOOKEEPER_HOME
    chown -R $HADOOP_USER:$HADOOP_USER $ZOOKEEPER_BUILDS

    echo "Zookeeper new conf files $ZOOKEEPER_CONF"
    rm -rf $ZOOKEEPER_CONF
    mkdir -p $ZOOKEEPER_CONF
    cp -r $ZOOKEEPER_SRC/conf/* $ZOOKEEPER_CONF/
}

zookeeper_build
zookeeper_install

