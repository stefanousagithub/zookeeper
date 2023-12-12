#!/bin/sh
BADUACLI="ba-dua-cli-0.6.0-all.jar"
BADUASER="../zookeeper-server/target/badua.ser"
CLASSES="../zookeeper-server/target/classes"
BADUAXML="../zookeeper-server/target/badua.xml"

java -jar ${BADUACLI} report    \
        -input ${BADUASER}      \
        -classes ${CLASSES}     \
        -show-classes           \
        -show-methods           \
        -xml ${BADUAXML}        \
