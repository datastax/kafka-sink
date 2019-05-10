#!/usr/bin/env bash

CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}

# start Graphite Instance
ctool launch -p bionic -i i2.2xlarge kc-perf 1
ctool perf_monitoring --install-only graphite -s /home/automaton/perf_monitoring kc-perf


ctool launch -p bionic -i m3.2xlarge kc-brokers 3
ctool run kc-brokers all "curl -O http://packages.confluent.io/archive/5.2/confluent-community-5.2.1-2.12.tar.gz"
ctool run kc-brokers all "mkdir confluent; tar xzf confluent-community-5.2.1-2.12.tar.gz -C confluent --strip-components=1"


# set broker ids
ctool run kc-brokers 0 "sed -i \"s/broker.id=.*/broker.id=0/\" confluent/etc/kafka/server.properties"
ctool run kc-brokers 1 "sed -i \"s/broker.id=.*/broker.id=1/\" confluent/etc/kafka/server.properties"
ctool run kc-brokers 2 "sed -i \"s/broker.id=.*/broker.id=2/\" confluent/etc/kafka/server.properties"

BROKER_FIRST_ADDRESS=`ctool info --public-ips kc-brokers -n 0`
BROKER_SECOND_ADDRESS=`ctool info --public-ips kc-brokers -n 1`
BROKER_THIRD_ADDRESS=`ctool info --public-ips kc-brokers -n 2`
GRAPHITE_ADDRESS=`ctool info --public-ips kc-perf -n 0`

# set broker ips in kafka server props
ctool run kc-brokers all "sed -i \"s/zookeeper.connect=.*/zookeeper.connect=$BROKER_FIRST_ADDRESS:2181,$BROKER_SECOND_ADDRESS:2181,$BROKER_THIRD_ADDRESS:2181/\" confluent/etc/kafka/server.properties"

# disable confluent metrics
ctool run kc-brokers all "sed -i \"s/confluent.support.metrics.enable=.*/confluent.support.metrics.enable=false/\" confluent/etc/kafka/server.properties"

# initLimit is timeouts ZooKeeper uses to limit the length of time the ZooKeeper servers in quorum have to connect to a leader
ctool run kc-brokers all "echo \"initLimit=5\" >> confluent/etc/kafka/zookeeper.properties"

# syncLimit limits how far out of date a server can be from a leader
ctool run kc-brokers all "echo \"syncLimit=2\" >> confluent/etc/kafka/zookeeper.properties"

# set ips of zookeeper nodes
ctool run kc-brokers all "echo \"server.0=$BROKER_FIRST_ADDRESS:2888:3888\" >> confluent/etc/kafka/zookeeper.properties"
ctool run kc-brokers all "echo \"server.1=$BROKER_SECOND_ADDRESS:2888:3888\" >> confluent/etc/kafka/zookeeper.properties"
ctool run kc-brokers all "echo \"server.2=$BROKER_THIRD_ADDRESS:2888:3888\" >> confluent/etc/kafka/zookeeper.properties"

# set zookeeper ids
ctool run kc-brokers 0 "mkdir /tmp/zookeeper/ -p; touch /tmp/zookeeper/myid; echo 0 >> /tmp/zookeeper/myid"
ctool run kc-brokers 1 "mkdir /tmp/zookeeper/ -p; touch /tmp/zookeeper/myid; echo 1 >> /tmp/zookeeper/myid"
ctool run kc-brokers 2 "mkdir /tmp/zookeeper/ -p; touch /tmp/zookeeper/myid; echo 2 >> /tmp/zookeeper/myid"

# Install graphite collectors on all kc-brokers
ctool perf_monitoring --install-only collectors --collect-only os --graphite-ip ${GRAPHITE_ADDRESS} kc-brokers #todo failed

# start zookeeper, kafka on all brokers, and schema registry on node0
ctool run kc-brokers all "confluent/bin/zookeeper-server-start confluent/etc/kafka/zookeeper.properties &> zookeeper.log &"
ctool run kc-brokers all "confluent/bin/kafka-server-start confluent/etc/kafka/server.properties &> kafka.log &"
ctool run kc-brokers 0 "./confluent/bin/schema-registry-start confluent/etc/schema-registry/schema-registry.properties &> schema-registry.log &"

ctool run kc-brokers all "sudo apt-get install -y maven"

ctool run kc-brokers 0 "confluent/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 2 --partitions 100 --topic json-stream --config retention.ms=-1 delete.topic.enable=true"

ctool run kc-brokers 0 "git clone https://github.com/datastax/kafka-examples.git"

# Produce 1_000_000_000 records to json-stream topic
ctool run kc-brokers 0 "cd kafka-examples/producers; mvn clean compile exec:java -Dexec.mainClass=json.JsonProducer"