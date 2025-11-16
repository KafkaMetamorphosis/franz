#!/bin/bash

# if host kafka-fleet, kafka-2, kafka-3 or kafka-ui does not exist, add them to /etc/hosts

kafka_hosts=("kafka-fleet" "kafka-2" "kafka-3", "kafka-ui")

for host in "${kafka_hosts[@]}"; do
    if ! grep -q "$host" /etc/hosts; then
        echo "127.0.0.1 $host" >> /etc/hosts
        echo "Adding 127.0.0.1 $host to /etc/hosts"
    else
        echo "Host $host already exists in /etc/host, skiping.."
    fi
done
