#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

# Garantit que le tmpdir de delta-rs est sur le même filesystem que les données
# (bind mount Docker), évitant l'erreur "Upload aborted" du rename cross-device.
mkdir -p /opt/spark/data/.tmp
export TMPDIR=/opt/spark/data/.tmp

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -p 7077
elif [[ $SPARK_WORKLOAD =~ "worker" ]];
# if $SPARK_WORKLOAD contains substring "worker". try 
# try "worker-1", "worker-2" etc.
then
  start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi
