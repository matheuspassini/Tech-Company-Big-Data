#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then

  # Only format HDFS if it's the first time (namenode directory doesn't exist)
  if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "First time initialization - formatting HDFS namenode"
    hdfs namenode -format
  else
    echo "HDFS already formatted - skipping format"
  fi

  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # Wait for namenode to be ready
  echo "Waiting for namenode to be ready..."
  while ! hdfs dfsadmin -report > /dev/null 2>&1; do
    sleep 2
  done

  hdfs dfs -mkdir -p /opt/spark/data
  hdfs dfs -mkdir -p /opt/spark/data/bronze_layer
  hdfs dfs -mkdir -p /opt/spark/data/silver_layer
  hdfs dfs -mkdir -p /opt/spark/data/gold_layer
  hdfs dfs -mkdir -p /data-lake-logs

  echo "Data folders created on HDFS"

  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data/bronze_layer

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
