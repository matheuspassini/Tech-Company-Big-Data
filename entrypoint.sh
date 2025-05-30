#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then

  hdfs namenode -format
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  hdfs dfs -mkdir -p /opt/spark/data
  hdfs dfs -mkdir -p /opt/spark/data/raw_layer
  hdfs dfs -mkdir -p /opt/spark/data/processed_layer
  hdfs dfs -mkdir -p /opt/spark/data/lineage_layer
  hdfs dfs -mkdir -p /data-lake-logs

  echo "Data folders created on HDFS"

  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data/raw_layer

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

  hdfs --daemon start datanode
  yarn --daemon start nodemanager

elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /data-lake-logs;
  do
    echo "spark-logs não existe ainda...criando"
    sleep 1;
  done
  echo "Exit loop"

  start-history-server.sh

fi

tail -f /dev/null
