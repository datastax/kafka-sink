# Kafka Sink for DataStax Enterprise

Kafka sink for transferring events/messages from Kafka topics to DSE

## How to use

First build the uber jar: `mvn clean package -DskipTests`

Edit the `conf/dse-sink.properties.sample` config file in this project to meet your needs,
or copy it out and edit elsewhere.

Update the plugin search path in the Connect worker config `config/connect-standalone.properties`
to include the uber-jar:

`plugin.path=<previous value>, <full path to repo>/target/kafka-connect-dse-<version>-SNAPSHOT.jar`

Run connect-standalone and specify the path to the config file:

`bin/connect-standalone.sh config/connect-standalone.properties ~/kafka-sink/conf/dse-sink.properties.sample`

In Confluent, you would do this:

`bin/confluent load dse-sink -d ~/kafka-sink/conf/dse-sink.properties`

## Mapping specification

See [the integration test](src/it/java/com/datastax/kafkaconnector/ccm/SimpleEndToEndCCMIT.java) for details.
