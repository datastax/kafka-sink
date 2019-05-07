#!/usr/bin/env bash
pyenv activate ctool-env

CONNECTOR_JAR_LOCATION=/Users/tomaszlelek/IntelliJ_workspace/kafka-sink/dist/target/kafka-connect-dse-1.1.0-SNAPSHOT.jar
KAFKA_SINK_REPO_LOCATION=/Users/tomaszlelek/IntelliJ_workspace/kafka-sink
DSE_CONNECTOR_VERSION=1.1.0-SNAPSHOT
LOGS_OUTPUT_DIRECTORY=/tmp/certify-confluent-tests

mkdir ${LOGS_OUTPUT_DIRECTORY}

echo "smoke test confluent 5.2"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.2" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.2.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_5.2.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.2.log
ctool destroy kct


echo "smoke test confluent 5.1"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.1" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.1.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_5.1.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.1.log
ctool destroy kct


echo "smoke test confluent 5.0"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.0" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.0.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_5.0.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.0.log
ctool destroy kct


echo "smoke test confluent 4.1"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "4.1" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_4.1.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_4.1.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_4.1.log
ctool destroy kct

echo "smoke test confluent 4.0"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "4.0" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_4.0.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_4.0.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_4.0.log
ctool destroy kct

echo "smoke test confluent 3.3"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "3.3" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_3.3.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_3.3.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_3.3.log
ctool destroy kct

echo "smoke test confluent 3.2"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "3.2" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_3.2.log"
ctool scp -r kct 0 /home/automaton/certify_confluent_3.2.log ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_3.2.log
ctool destroy kct