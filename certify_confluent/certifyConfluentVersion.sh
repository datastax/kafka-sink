#!/bin/sh

# This is a script to run the Avro example in the DataStax Kafka Examples Github Repository
#
# https://github.com/datastax/kafka-examples/tree/master/producers/src/main/java/avro
#
# Prerequisites
# 1. Java 8+ must be installed
# 2. Maven must be installed
# 3. Use TOPIC_NAME env var to set the Kafka Topic that will be created
# 4. Use TOTAL_RECORDS env var to control the number of records written to Kafka
CONFLUENT_HOME=/tmp/confluent
CONNECTOR_HOME=/tmp/cass-sink-connector
DSE_HOME=/tmp/dse
TOTAL_RECORDS=1000
TOPIC_NAME="avro-stream"


# If running with DataStax Cloud:
# Set CLOUD_USERNAME to username of your apollo constellation db
# Set CLOUD_PASSWORD to password CLOUD_PASSWORD of your apollo constellation db
# Set CLOUD_KEYSPACE to a keyspace of your apollo constellation db
CLOUD_USERNAME=user
CLOUD_PASSWORD=password
CLOUD_KEYSPACE=ks1

CONFLUENT_VERSION=$1
CASS_SINK_CONNECTOR_VERSION=$2
IS_CLOUD=$3

wait_for_port () {
  SVCNAME=$1
  PORT=$2

  while ! netstat -an | grep LIST | grep $PORT > /dev/null 2>&1 ; do
    echo "[$(date '+%H:%M:%S')]: Waiting for $SVCNAME to listen on port $PORT"
    sleep 2
  done
}

assert_confluent_home () {
  if [ -z "$CONFLUENT_HOME" ] ; then
    echo "You must set the CONFLUENT_HOME env var to the location of the Confluent installation before performing this operation."
    exit 1
  fi
}

assert_connector_home () {
  if [ -z "$CONNECTOR_HOME" ] ; then
    echo "You must set the CONNECTOR_HOME env var to the location of the Cassandra Sink connector installation before performing this operation."
    exit 1
  fi
}

assert_dse_home () {
  if [ -z "$DSE_HOME" ] ; then
    echo "You must set the DSE_HOME env var to the location of the Cassandra or DSE installation before performing this operation."
    exit 1
  fi
}

maybe_set_topic_name () {
  if [ -z "$TOPIC_NAME" ] ; then
    TOPIC_NAME="avro-stream"
  fi
}

maybe_set_total_records () {
  if [ -z "$TOTAL_RECORDS" ] ; then
    TOTAL_RECORDS=1000
  fi
}

maybe_set_dse_connector_version () {
  if [ -z "$CASS_SINK_CONNECTOR_VERSION" ] ; then
    CASS_SINK_CONNECTOR_VERSION=1.0.0
  fi
}

confluent_major () {
  echo $CONFLUENT_HOME | sed -e 's/.*-//' -e 's/\..*//'
}

install_confluent () {
    if [ "$CONFLUENT_VERSION" = "5.2" ]
    then
        install_confluent_5_2
    elif [ "$CONFLUENT_VERSION" = "5.1" ]
    then
        install_confluent_5_1
    elif [ "$CONFLUENT_VERSION" = "5.0" ]
    then
        install_confluent_5_0
    elif [ "$CONFLUENT_VERSION" = "4.1" ]
    then
        install_confluent_4_1
    elif [ "$CONFLUENT_VERSION" = "4.0" ]
    then
        install_confluent_4_0
    elif [ "$CONFLUENT_VERSION" = "3.3" ]
    then
        install_confluent_3_3
    fi
}


install_confluent_3_3(){
    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/3.3/confluent-3.3.2-2.11.tar.gz && wait
	tar xzf confluent-3.3.2-2.11.tar.gz -C $CONFLUENT_HOME --strip-components=1

}

install_confluent_4_0(){
    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/4.0/confluent-4.0.2-2.11.tar.gz && wait
	tar xzf confluent-4.0.2-2.11.tar.gz -C $CONFLUENT_HOME --strip-components=1

}

install_confluent_4_1(){
    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/4.1/confluent-4.1.2-2.11.tar.gz && wait
	tar xzf confluent-4.1.2-2.11.tar.gz -C $CONFLUENT_HOME --strip-components=1

}

install_confluent_5_0 () {

    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/5.0/confluent-5.0.2-2.11.tar.gz && wait
	tar xzf confluent-5.0.2-2.11.tar.gz -C $CONFLUENT_HOME --strip-components=1
}

install_confluent_5_1 () {

    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/5.1/confluent-5.1.2-2.11.tar.gz && wait
	tar xzf confluent-5.1.2-2.11.tar.gz -C $CONFLUENT_HOME --strip-components=1
}

install_confluent_5_2 () {

    echo
	echo "-----------------------------------------------"
	echo "--- INSTALLING CONFLUENT $CONFLUENT_VERSION ---"
	echo "-----------------------------------------------"
	echo "version: "
	mkdir $CONFLUENT_HOME
	curl -O http://packages.confluent.io/archive/5.2/confluent-community-5.2.1-2.12.tar.gz && wait
	tar xzf confluent-community-5.2.1-2.12.tar.gz -C $CONFLUENT_HOME --strip-components=1
}

install_dse () {
	echo
	echo "----------------------------------------"
	echo "---  INSTALLING DATASTAX ENTERPRISE  ---"
	echo "----------------------------------------"
	mkdir $DSE_HOME; mkdir $DSE_HOME/logs; mkdir $DSE_HOME/data;
	curl -L -O https://downloads.datastax.com/enterprise/dse.tar.gz && wait
	tar xzf dse.tar.gz -C $DSE_HOME --strip-components=1
	sed -i 's/\/var\/lib\/cassandra\/*//g' $DSE_HOME/resources/cassandra/conf/cassandra.yaml
	sed -i 's/\/var\/log\/cassandra\/*//g' $DSE_HOME/resources/cassandra/conf/cassandra.yaml
}

install_kafka_examples () {
	echo
	echo "----------------------------------------"
	echo "---  CLONING KAFKA-EXAMPLES GITHUB   ---"
	echo "----------------------------------------"
	git clone https://github.com/datastax/kafka-examples.git kafka-examples
}

stop_confluent(){
	echo
	echo "----------------------------------------"
	echo "---         STOPPING ZOOKEEPER       ---"
	echo "----------------------------------------"
    ${CONFLUENT_HOME}/bin/zookeeper-server-stop

    echo
	echo "----------------------------------------"
	echo "---       STOPPING KAFKA BROKER      ---"
	echo "----------------------------------------"
	${CONFLUENT_HOME}/bin/kafka-server-stop

	echo
	echo "----------------------------------------"
	echo "---    STOPPING SCHEMA REGISTRY    ---"
	echo "----------------------------------------"
	${CONFLUENT_HOME}/bin/schema-registry-stop

	kill -9 `lsof -t -i:8083`
}

start_confluent () {
	mkdir -p $CONFLUENT_HOME/logs

	echo
	echo "----------------------------------------"
	echo "---         STARTING ZOOKEEPER       ---"
	echo "----------------------------------------"
	$CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties >> $CONFLUENT_HOME/logs/zookeeper.log 2>&1 &

	# Wait for zookeeper to be up.
	wait_for_port zookeeper 2181

	sleep 10

	echo
	echo "----------------------------------------"
	echo "---       STARTING KAFKA BROKER      ---"
	echo "----------------------------------------"
	$CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties >> $CONFLUENT_HOME/logs/kafka-server.log 2>&1 &

	# Wait for broker to be up.
	wait_for_port broker 9092

	echo
	echo "----------------------------------------"
	echo "---     STARTING SCHEMA REGISTRY     ---"
	echo "----------------------------------------"
	$CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties >> $CONFLUENT_HOME/logs/schema-registry.log 2>&1 &

	# Wait for schema registry to be up.
	wait_for_port schema-registry 8081
}

start_dse () {
  if [ "$IS_CLOUD" = "false" ]
  then
	    echo
	    echo "----------------------------------------"
	    echo "---   STARTING DATASTAX ENTERPRISE   ---"
	    echo "----------------------------------------"
	    $DSE_HOME/bin/dse cassandra >> $DSE_HOME/logs/startup.log 2>&1 &

	    # Wait for DSE to be up.
	    wait_for_port dse 9042
  fi
}

start_distributed_worker () {
	echo
	echo "----------------------------------------"
	echo "---   STARTING KAFKA CONNECT WORKER  ---"
	echo "----------------------------------------"
	plugin_path=${CONFLUENT_HOME}/share/,${CONNECTOR_HOME}/kafka-connect-dse-${CASS_SINK_CONNECTOR_VERSION}.jar
	sed -i "s#plugin\.path.*#plugin\.path=$plugin_path#" kafka-examples/producers/src/main/java/avro/connect-distributed-avro.properties
	$CONFLUENT_HOME/bin/connect-distributed kafka-examples/producers/src/main/java/avro/connect-distributed-avro.properties >> $CONFLUENT_HOME/logs/worker-avro-example.log 2>&1 &

	# Wait for Worker to be up.
	wait_for_port ConnectDistributed 8083
}

start_connector () {
    echo
    echo "----------------------------------------"
    echo "---    STARTING DATASTAX CONNECTOR   ---"
	echo "----------------------------------------"

    if [ "$IS_CLOUD" = "false" ]
    then
        curl -X POST -H "Content-Type: application/json" -d @kafka-examples/producers/src/main/java/avro/cassandra-sink-avro.json "http://localhost:8083/connectors"
    else
        sed -i "s/CLOUD_USERNAME/$CLOUD_USERNAME/g" /home/automaton/kafka-examples/producers/src/main/java/avro/cloud/cassandra-sink-avro-cloud.json
        sed -i "s/CLOUD_PASSWORD/$CLOUD_PASSWORD/g" /home/automaton/kafka-examples/producers/src/main/java/avro/cloud/cassandra-sink-avro-cloud.json
        sed -i "s/CLOUD_KEYSPACE/$CLOUD_KEYSPACE/g" /home/automaton/kafka-examples/producers/src/main/java/avro/cloud/cassandra-sink-avro-cloud.json
        curl -X POST -H "Content-Type: application/json" -d @/home/automaton/kafka-examples/producers/src/main/java/avro/cloud/cassandra-sink-avro-cloud.json "http://localhost:8083/connectors"
    fi

}

start_producer () {
	echo
	echo "----------------------------------------"
	echo "---      STARTING AVRO PRODUCER      ---"
	echo "----------------------------------------"
	cd kafka-examples/producers; mvn clean compile exec:java -Dexec.mainClass=avro.AvroProducer -Dexec.args="$TOPIC_NAME $TOTAL_RECORDS"
}

create_kafka_topic () {
	echo
	echo "----------------------------------------"
	echo "--- CREATING KAFKA TOPIC $TOPIC_NAME ---"
	echo "----------------------------------------"
	$CONFLUENT_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic $TOPIC_NAME --config retention.ms=-1
}

verify_records_in_kafka () {
	echo
	echo "----------------------------------------"
	echo "---    VERIFYING RECORDS IN KAFKA    ---"
	echo "----------------------------------------"
	$CONFLUENT_HOME/bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --from-beginning --property print.key=true --max-messages 5 --topic $TOPIC_NAME
}

create_dse_schema () {
    if [ "$IS_CLOUD" = "false" ]
    then
	   echo
	   echo "----------------------------------------"
	   echo "---       CREATING DSE SCHEMA        ---"
	   echo "----------------------------------------"
	   echo
	   echo `cat kafka-examples/producers/src/main/java/avro/create_avro_table_udt.cql`

	   $DSE_HOME/bin/cqlsh -f kafka-examples/producers/src/main/java/avro/create_avro_table_udt.cql
	else
	   cd /home/automaton/secure-bundle
	   $DSE_HOME/bin/cqlsh --cqlshrc="cqlshrc" --username ${CLOUD_USERNAME} --password ${CLOUD_PASSWORD} -k ${CLOUD_KEYSPACE} -f /home/automaton/kafka-examples/producers/src/main/java/avro/cloud/create_avro_table_udt.cql
	   cd ~
	fi
}

verify_rows_in_dse () {
    if [ "$IS_CLOUD" = "false" ]
    then
	    echo
	    echo "----------------------------------------"
	    echo "---      VERIYING ROWS IN DSE        ---"
	    echo "----------------------------------------"

	    $DSE_HOME/bin/cqlsh -e "select * from kafka_examples.avro_udt_table limit 5;"

    	echo "Counting rows in DSE using -- $DSE_HOME/bin/dsbulk count -k kafka_examples -t avro_udt_table --"

    	$DSE_HOME/bin/dsbulk count -k kafka_examples -t avro_udt_table
	else
	    cd /home/automaton/secure-bundle
        echo `$DSE_HOME/bin/cqlsh --cqlshrc="cqlshrc" --username ${CLOUD_USERNAME} --password ${CLOUD_PASSWORD} -e "select count(*) from ${CLOUD_KEYSPACE}.avro_udt_table"`
        cd ~
	fi
}

# check that all needed options are set
assert_confluent_home
assert_dse_home
assert_connector_home
maybe_set_topic_name
maybe_set_total_records
maybe_set_dse_connector_version

# Install Confluent, DataStax Connector, DSE, and Kafka Examples
install_confluent
install_dse
install_kafka_examples

# Start Confluent, DSE, Kafka Connect Worker, and DataStax Connector
start_confluent
start_dse
start_distributed_worker
start_connector

# Create the Kafka Topic and DSE Schema
create_kafka_topic
create_dse_schema

# Start Avro Producer
start_producer

# Verify that the producer wrote records to Kafka and that those rows show up in DSE
verify_records_in_kafka
verify_rows_in_dse
