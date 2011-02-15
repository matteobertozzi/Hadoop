#!/bin/sh

AVRO_SRC="/usr/local/src/apache/avro"
AVRO_SRC_JAVA="$AVRO_SRC/lang/java/"
AVRO_SRC_PY="$AVRO_SRC/lang/py"
AVRO_SRC_C="$AVRO_SRC/lang/c"

AVRO_HOME="/usr/local/avro"

avro_install() {
    # Compile and Install java avro
    cd $AVRO_SRC_JAVA
    mvn clean dependency:copy-dependencies package -DskipTests > /dev/null
    mkdir -p $AVRO_HOME
    cp $AVRO_SRC_JAVA/tools/target/dependency/*.jar $AVRO_HOME/

    # Compile and install python avro
    cd $AVRO_SRC_PY
    ant
    cd $AVRO_SRC_PY/build
    python setup.py install

    # Compile and install avro C
    cd $AVRO_SRC_C
    cmake CMakeLists.txt
    make && make install
}

avro_install
