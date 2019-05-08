#!/usr/bin/env bash
# This script validates the correctness of kafka-connector-dse against different confluent versions.
# It will start the Openstack instance for every Confluent version, set up the Kafka ecosystem + DSE and start the connector.
# The output logs with a status of verification will be copied to $LOGS_OUTPUT_DIRECTORY on your local machine.
# After every run the open-stack instance is destroyed and the new one for the next confluent version is created
# The process is repeated for confluent versions: 5.2, 5.1, 5.0, 4.1, 4.0, 3.3
# You need to specify the location to the jar file that will be used to perform a test using CONNECTOR_JAR_LOCATION.
# Version of that JAR should be the same as the DSE_CONNECTOR_VERSION parameter (that you can change as well).
# You need to specify the location of the repo with kafka-connect project via KAFKA_SINK_REPO_LOCATION parameter.
# This is needed for copying the running script into a ctool instance.
# To use ctool properly the virtual-env is used.
# Set CTOOL_ENV to name of the virtual-env inside of which you have ctool setup.

CONNECTOR_JAR_LOCATION=path-to-connector-jar-on-your-local-system
KAFKA_SINK_REPO_LOCATION=/your-local-location/kafka-sink
DSE_CONNECTOR_VERSION=1.1.0-SNAPSHOT
LOGS_OUTPUT_DIRECTORY=/tmp/certify-confluent-tests
CTOOL_ENV=ctool-env

pyenv activate ${CTOOL_ENV}
ctool destroy kct

mkdir ${LOGS_OUTPUT_DIRECTORY}

echo "smoke test confluent 5.2"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.2" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.2.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.2.log /home/automaton/certify_confluent_5.2.log
ctool destroy kct


echo "smoke test confluent 5.1"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.1" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.1.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.1.log /home/automaton/certify_confluent_5.1.log
ctool destroy kct


echo "smoke test confluent 5.0"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.0" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_5.0.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.0.log /home/automaton/certify_confluent_5.0.log
ctool destroy kct


echo "smoke test confluent 4.1"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "4.1" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_4.1.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_4.1.log /home/automaton/certify_confluent_4.1.log
ctool destroy kct

echo "smoke test confluent 4.0"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "4.0" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_4.0.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_4.0.log /home/automaton/certify_confluent_4.0.log
ctool destroy kct

echo "smoke test confluent 3.3"
ctool --provider=ironic launch -p devtools-ironic kct 1
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluent.sh /home/automaton
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluent.sh; chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "3.3" "${DSE_CONNECTOR_VERSION}" &> certify_confluent_3.3.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_3.3.log /home/automaton/certify_confluent_3.3.log
ctool destroy kct