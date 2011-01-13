#!/bin/bash

HADOOP_USER="hadoop"

HADOOP_SRC="/usr/local/src/hadoop"

HBASE_SRC="$HADOOP_SRC/hbase"
HBASE_BUILDS="/usr/local/hbase-builds"
HBASE_BUILDNR="$HBASE_BUILDS/.buildnr"
HBASE_HOME="/usr/local/hbase"
HBASE_CONF="/root/hbase-conf"

hbase_build() {
    echo "Build HBase"
    cd $HBASE_SRC
    mvn clean dependency:copy-dependencies package -DskipTests > /dev/null
}

hbase_install() {
    mkdir -p $HBASE_BUILDS
    touch $HBASE_BUILDNR
    build_number=$((`cat $HBASE_BUILDNR` + 1))
    echo $build_number > $HBASE_BUILDNR

    echo "HBase Install $build_number"
    hbase_build="$HBASE_BUILDS/hbase-$build_number"
    mkdir -p $hbase_build
    rm -f $HBASE_HOME
    ln -s $hbase_build $HBASE_HOME

    ln -s /etc/hbase $HBASE_HOME/conf
    mkdir -p $HBASE_HOME/classes
    mkdir -p $HBASE_HOME/bin
    mkdir -p $HBASE_HOME/lib

    cp -r $HBASE_SRC/target/hbase-*.jar $HBASE_HOME/

    cp -r $HBASE_SRC/bin/* $HBASE_HOME/bin/

    cp -r $HBASE_SRC/target/classes/* $HBASE_HOME/classes/

    cp $HBASE_SRC/target/dependency/*.jar $HBASE_HOME/lib/
    cp -r $HBASE_SRC/src/main/ruby $HBASE_HOME/lib/

    echo "$hbase_build"
    echo $HBASE_HOME

    chown -R $HADOOP_USER:$HADOOP_USER $HBASE_HOME
    chown -R $HADOOP_USER:$HADOOP_USER $HBASE_BUILDS

    echo "HBase new conf files $HBASE_CONF"
    rm -rf $HBASE_CONF
    mkdir -p $HBASE_CONF
    cp -r $HBASE_SRC/conf/* $HBASE_CONF/
}



hbase_build
hbase_install

