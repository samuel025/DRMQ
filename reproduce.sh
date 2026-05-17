#!/bin/bash
rm -rf data1 data2 data3
nohup java -cp drmq-broker/target/classes:drmq-protocol/target/classes:drmq-protocol/target/generated-sources/protobuf/java:/home/samuel/.m2/repository/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-classic/1.4.14/logback-classic-1.4.14.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-core/1.4.14/logback-core-1.4.14.jar:/home/samuel/.m2/repository/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar:/home/samuel/.m2/repository/io/netty/netty-all/4.1.107.Final/netty-all-4.1.107.Final.jar com.drmq.broker.BrokerServer --port 9091 --data-dir data1 --id node1 --peers node2:localhost:9092,node3:localhost:9093 > node1.log 2>&1 &
PID1=$!

sleep 2

nohup java -cp drmq-broker/target/classes:drmq-protocol/target/classes:drmq-protocol/target/generated-sources/protobuf/java:/home/samuel/.m2/repository/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-classic/1.4.14/logback-classic-1.4.14.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-core/1.4.14/logback-core-1.4.14.jar:/home/samuel/.m2/repository/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar:/home/samuel/.m2/repository/io/netty/netty-all/4.1.107.Final/netty-all-4.1.107.Final.jar com.drmq.broker.BrokerServer --port 9092 --data-dir data2 --id node2 --peers node1:localhost:9091,node3:localhost:9093 > node2.log 2>&1 &
PID2=$!

sleep 2

nohup java -cp drmq-broker/target/classes:drmq-protocol/target/classes:drmq-protocol/target/generated-sources/protobuf/java:/home/samuel/.m2/repository/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-classic/1.4.14/logback-classic-1.4.14.jar:/home/samuel/.m2/repository/ch/qos/logback/logback-core/1.4.14/logback-core-1.4.14.jar:/home/samuel/.m2/repository/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar:/home/samuel/.m2/repository/io/netty/netty-all/4.1.107.Final/netty-all-4.1.107.Final.jar com.drmq.broker.BrokerServer --port 9093 --data-dir data3 --id node3 --peers node1:localhost:9091,node2:localhost:9092 > node3.log 2>&1 &
PID3=$!

sleep 5

kill $PID1 $PID2 $PID3
cat node3.log
