#!/usr/bin/env bash
# This script validates the correctness of kafka-connector-dse against Datastax Cloud https://apollo.datastax.com.
# It will start the Openstack instance, set up the Kafka ecosystem and start the connector.
# The output logs with a status of verification will be copied to $LOGS_OUTPUT_DIRECTORY on your local machine.
# After run the open-stack instance is destroyed.
# You need to specify the location to the jar file that will be used to perform a test using CONNECTOR_JAR_LOCATION.
# Version of that JAR should be the same as the DSE_CONNECTOR_VERSION parameter (that you can change as well).
# You need to specify the location of the repo with kafka-connect project via KAFKA_SINK_REPO_LOCATION parameter.
# This is needed for copying the running script into a ctool instance.
# To use ctool properly the virtual-env is used.
# Set CTOOL_ENV to name of the virtual-env inside of which you have ctool setup.
# Set CLOUD_SECURE_BUNDLE_LOCATION to your secure-connect.zip bundle with cloud credentials
# SET CLOUD_SECURE_BUNDLE_FILE_NAME to name of your secure-connect.zip
# Set CLOUD_USERNAME to username of your apollo constellation db
# Set CLOUD_PASSWORD to password CLOUD_PASSWORD of your apollo constellation db
# Set CLOUD_KEYSPACE to a keyspace of your apollo constellation db
# Execute all CLQs from the cloud/create_avro_table_udt.cql file on your cloud instance

CONNECTOR_JAR_LOCATION=/Users/tomaszlelek/IntelliJ_workspace/kafka-sink/dist/target/kafka-connect-dse-1.2.0-SNAPSHOT.jar
KAFKA_SINK_REPO_LOCATION=/Users/tomaszlelek/IntelliJ_workspace/kafka-sink
DSE_CONNECTOR_VERSION=1.2.0-SNAPSHOT
LOGS_OUTPUT_DIRECTORY=/tmp/certify-confluent-tests
CTOOL_ENV=ctool-env
CLOUD_SECURE_BUNDLE_LOCATION=/Users/tomaszlelek/Downloads/secure-connect-db1.zip
CLOUD_SECURE_BUNDLE_FILE_NAME="secure-connect-db1.zip"
CLOUD_USERNAME=user
CLOUD_PASSWORD=password
CLOUD_KEYSPACE=ks1

pyenv activate ${CTOOL_ENV}
ctool destroy kct

mkdir ${LOGS_OUTPUT_DIRECTORY}

sed -i "s/CLOUD_USERNAME/$CLOUD_USERNAME/g" cloud/dse-sink-avro-cloud.json
sed -i "s/CLOUD_PASSWORD/$CLOUD_PASSWORD/g" cloud/dse-sink-avro-cloud.json
sed -i "s/CLOUD_KEYSPACE/$CLOUD_KEYSPACE/g" cloud/dse-sink-avro-cloud.json

echo "smoke test confluent 5.2"
ctool launch kct 1
ctool run kct all "sudo apt-get install -y maven"
ctool run kct "mkdir /tmp/dse-connector"
ctool scp -R kct 0 ${CONNECTOR_JAR_LOCATION} /tmp/dse-connector
ctool scp -R kct 0 ${KAFKA_SINK_REPO_LOCATION}/certify_confluent/certifyConfluentVersion.sh /home/automaton
ctool scp -R kct 0 ${CLOUD_SECURE_BUNDLE_LOCATION} /home/automaton
ctool scp -R kct 0 cloud/dse-sink-avro-cloud.json /home/automaton
ctool run --sudo kct "mv /home/automaton/${CLOUD_SECURE_BUNDLE_FILE_NAME} /home/automaton/secure-bundle.zip"
ctool run --sudo kct "chmod 777 /home/automaton/certifyConfluentVersion.sh"
ctool run kct "~/certifyConfluentVersion.sh "5.2" "${DSE_CONNECTOR_VERSION}" "true" &> certify_confluent_5.2_cloud.log"
ctool scp -r kct 0 ${LOGS_OUTPUT_DIRECTORY}/certify_confluent_5.2_cloud.log /home/automaton/certify_confluent_5.2_cloud.log
ctool destroy kct