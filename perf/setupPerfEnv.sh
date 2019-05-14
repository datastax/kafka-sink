#!/usr/bin/env bash

# MAX_POOL_RECORDS will be set as the consumer.max.poll.records in the connect-distributed.properties file.
# CONNECTOR_TYPE you can start this script with avro or json parameter value.
# The connect converters will be configured accordingly.

MAX_POOL_RECORDS=$1
CONNECTOR_TYPE=$2
CONNECTOR_JAR_LOCATION=/Users/tomaszlelek/IntelliJ_workspace/kafka-sink/dist/target/kafka-connect-dse-1.1.0-SNAPSHOT.jar
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
ctool perf_monitoring --install-only collectors --collect-only os --graphite-ip $GRAPHITE_ADDRESS kc-brokers

# start zookeeper, kafka on all brokers, and schema registry on node0
ctool run kc-brokers all "confluent/bin/zookeeper-server-start confluent/etc/kafka/zookeeper.properties &> zookeeper.log &"
ctool run kc-brokers all "confluent/bin/kafka-server-start confluent/etc/kafka/server.properties &> kafka.log &"

ctool run kc-brokers all "echo \"kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092\" >>confluent/etc/schema-registry/schema-registry.properties"
ctool run kc-brokers all "./confluent/bin/schema-registry-start confluent/etc/schema-registry/schema-registry.properties &> schema-registry.log &"

ctool run kc-brokers all "sudo apt-get install -y maven"

ctool run kc-brokers 0 "confluent/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 2 --partitions 100 --topic json-stream --config retention.ms=-1 delete.topic.enable=true"

ctool run kc-brokers 0 "confluent/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 2 --partitions 100 --topic avro-stream --config retention.ms=-1 delete.topic.enable=true"

ctool run kc-brokers 0 "git clone https://github.com/datastax/kafka-examples.git"


# -------- Kafka Connect L Setup --------
ctool launch -p bionic -i c3.2xlarge kc-connect-l 3
ctool run kc-connect-l all "curl -O http://packages.confluent.io/archive/5.2/confluent-community-5.2.1-2.12.tar.gz"
ctool run kc-connect-l all "mkdir confluent; tar xzf confluent-community-5.2.1-2.12.tar.gz -C confluent --strip-components=1"

# Setup worker Distributed Properties
ctool run kc-connect-l all "echo \"confluent.support.metrics.enable=false\" >> confluent/etc/kafka/connect-distributed.properties"
ctool run kc-connect-l all "echo \"consumer.max.poll.records=$MAX_POOL_RECORDS\" >> confluent/etc/kafka/connect-distributed.properties"

ctool run kc-connect-l all "sed -i \"s/^group.id=.*/group.id=kc-connect-s-group/\" confluent/etc/kafka/connect-distributed.properties"

ctool run kc-connect-l all "sed -i \"s/^bootstrap.servers=.*/bootstrap.servers=$BROKER_FIRST_ADDRESS:9092,$BROKER_SECOND_ADDRESS:9092,$BROKER_THIRD_ADDRESS:9092/\" confluent/etc/kafka/connect-distributed.properties"

# Configure Metrics
ctool perf_monitoring --install-only collectors --collect-only os --graphite-ip $GRAPHITE_ADDRESS kc-connect-l

ctool run kc-connect-l all "curl -O http://central.maven.org/maven2/org/jmxtrans/agent/jmxtrans-agent/1.2.6/jmxtrans-agent-1.2.6.jar"
ctool scp kc-connect-l 0 kafka/kafka-connect-metrics-0.xml .
ctool scp kc-connect-l 1 kafka/kafka-connect-metrics-1.xml .
ctool scp kc-connect-l 2 kafka/kafka-connect-metrics-2.xml .

# Copy connector JAR
ctool scp -R kc-connect-l all ${CONNECTOR_JAR_LOCATION} kafka-connect-dse.jar
plugin_path=/home/automaton/confluent/share,/home/automaton/kafka-connect-dse.jar
ctool run kc-connect-l all "sed -i \"s#plugin\.path.*#plugin\.path=$plugin_path\n#\" confluent/etc/kafka/connect-distributed.properties"


CONNECT_FIRST_ADDRESS=`ctool info --public-ips kc-connect-l -n 0`
CONNECT_SECOND_ADDRESS=`ctool info --public-ips kc-connect-l -n 1`
CONNECT_THIRD_ADDRESS=`ctool info --public-ips kc-connect-l -n 2`

# Enable JMX on connect workers (inserts at line 72 - when incrementing confluent version be careful to validate if this is a proper line)
ctool run kc-connect-l 0 "sed -i '72i KAFKA_JMX_OPTS=\"-javaagent:/home/automaton/jmxtrans-agent-1.2.6.jar=/home/automaton/kafka-connect-metrics-0.xml -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$CONNECT_FIRST_ADDRESS -Dcom.sun.management.jmxremote.port=7199\"' confluent/bin/connect-distributed"
ctool run kc-connect-l 1 "sed -i '72i KAFKA_JMX_OPTS=\"-javaagent:/home/automaton/jmxtrans-agent-1.2.6.jar=/home/automaton/kafka-connect-metrics-1.xml -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$CONNECT_SECOND_ADDRESS -Dcom.sun.management.jmxremote.port=7199\"' confluent/bin/connect-distributed"
ctool run kc-connect-l 2 "sed -i '72i KAFKA_JMX_OPTS=\"-javaagent:/home/automaton/jmxtrans-agent-1.2.6.jar=/home/automaton/kafka-connect-metrics-2.xml -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$CONNECT_THIRD_ADDRESS -Dcom.sun.management.jmxremote.port=7199\"' confluent/bin/connect-distributed"

if [ "$CONNECTOR_TYPE" = "json" ]
then
    setup_json_convertes
elif [ "$CONNECTOR_TYPE" = "avro" ]
then
    setup_avro_converters
fi

setup_json_convertes(){
    ctool run kc-connect-l all "sed -i \"s/^key.converter=.*/key.converter=org.apache.kafka.connect.storage.StringConverter/\" confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "sed -i \"s/^key.converter.schemas.enable=.*/key.converter.schemas.enable=false/\" confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "sed -i \"s/^value.converter.schemas.enable=.*/value.converter.schemas.enable=false/\" confluent/etc/kafka/connect-distributed.properties"
}

setup_avro_converters(){
    ctool run kc-connect-l all "sed -i \"s/^key.converter=.*/key.converter=io.confluent.connect.avro.AvroConverter/\" confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "sed -i \"s/^value.converter=.*/value.converter=io.confluent.connect.avro.AvroConverter/\" confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "echo \"key.converter.schema.registry.url=http://$BROKER_FIRST_ADDRESS:8081,http://$BROKER_SECOND_ADDRESS:8081,http://$BROKER_THIRD_ADDRESS:8081\" >> confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "echo \"value.converter.schema.registry.url=http://$BROKER_FIRST_ADDRESS:8081,http://$BROKER_SECOND_ADDRESS:8081,http://$BROKER_THIRD_ADDRESS:8081\" >> confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "sed -i \"s/^key.converter.schemas.enable=.*/key.converter.schemas.enable=true/\" confluent/etc/kafka/connect-distributed.properties"
    ctool run kc-connect-l all "sed -i \"s/^value.converter.schemas.enable=.*/value.converter.schemas.enable=true/\" confluent/etc/kafka/connect-distributed.properties"
}

ctool run kc-connect-l all "confluent/bin/connect-distributed confluent/etc/kafka/connect-distributed.properties &> worker.log &"


# DSE Cluster Setup
ctool launch -p bionic -i m3.2xlarge kc-dse 5
ctool install -v 6.0.4 -i tar -n 8 -x kc-dse-topology.json kc-dse enterprise
ctool yaml -o set -k allocate_tokens_for_local_replication_factor -v 3 kc-dse all
ctool run kc-dse 0 "dse cassandra &> startup.log &"; sleep 120
ctool run kc-dse 1 "dse cassandra &> startup.log &"; sleep 120
ctool run kc-dse 2 "dse cassandra &> startup.log &"; sleep 120
ctool run kc-dse 3 "dse cassandra &> startup.log &"; sleep 120
ctool run kc-dse 4 "dse cassandra &> startup.log &"; sleep 120
ctool perf_monitoring --install-only collectors --graphite-ip $GRAPHITE_ADDRESS kc-dse
ctool scp -R kc-dse 0 setup_dse_schema.cql .
ctool run kc-dse 0 "dse/bin/cqlsh -f setup_dse_schema.cql"

stop_kafka_broker_services(){
    # kill kafka connect-worker
    ctool run kc-connect all "kill -9 `lsof -t -i:8083`"
    # kill kc-brokers services
    ctool run kc-brokers all "kill -9 `lsof -t -i:8081`"
    ctool run kc-brokers all "kill -9 `lsof -t -i:2181`"
    ctool run kc-brokers all "kill -9 `lsof -t -i:9092`"
}

destroy_env(){
    ctool destroy kc-connect-l
    ctool destroy kc-brokers
    ctool destroy kc-dse
    ctool destroy kc-perf
}