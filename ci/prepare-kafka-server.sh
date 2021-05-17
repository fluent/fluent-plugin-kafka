#!/bin/sh

export KAFKA_OPTS=-Dzookeeper.4lw.commands.whitelist=ruok
/usr/bin/zookeeper-server-start /etc/kafka/zookeeper.properties  &
N_POLLING=30
n=1
while true ; do
    sleep 1
    status=$(echo ruok | nc localhost 2181)
    if [ "$status" = "imok" ]; then
	break
    fi
    n=$((n + 1))
    if [ $n -ge $N_POLLING ]; then
	echo "failed to get response from zookeeper-server"
	exit 1
    fi
done
/usr/bin/kafka-server-start /etc/kafka/server.properties &
n=1
while true ; do
    sleep 1
    status=$(/usr/bin/zookeeper-shell localhost:2181 ls /brokers/ids | sed -n 6p)
    if [ "$status" = "[0]" ]; then
	break
    fi
    n=$((n + 1))
    if [ $n -ge $N_POLLING ]; then
	echo "failed to get response from kafka-server"
	exit 1
    fi
done
/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
