#!/usr/bin/env bash

JAVA_OPS="-XX:InitialHeapSize=3221225472 -XX:MaxDirectMemorySize=209715200 -XX:MaxHeapSize=3221225472 -XX:MaxNewSize=1073741824 -XX:MaxTenuringThreshold=6 -XX:NewSize=1073741824 -XX:OldPLABSize=16 -XX:OldSize=2147483648 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
jarPath="./target/sync-1.0.jar"

serverIP="192.168.1.3"


timestamp=$(date +%s)
echo "client start: "$timestamp

java $JAVA_OPS -cp $jarPath com.alibaba.middleware.race.sync.Client ${serverIP}


timestamp=$(date +%s)
echo "client end: "$timestamp