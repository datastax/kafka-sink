# Apache Kafka Sink for Apache Cassandra and DataStax Enterprise

An Apache Kafka sink for transferring events/messages from Kafka topics to Apache Cassandra (R) or
DataStax Enterprise (DSE).

## How to use

First build the uber jar: `mvn clean package -DskipTests`

Edit the `conf/cassandra-sink.properties.sample` config file in this project to meet your needs,
or copy it out and edit elsewhere.

Update the plugin search path in the Connect worker config `config/connect-standalone.properties`
to include the uber-jar:

`plugin.path=<previous value>, <full path to repo>/target/kafka-connect-cassandra-<version>-SNAPSHOT.jar`

Run connect-standalone and specify the path to the config file:

`bin/connect-standalone.sh config/connect-standalone.properties ~/kafka-sink/conf/cassandra-sink.properties.sample`

In Confluent, you would do this:

`bin/confluent load cassandra-sink -d ~/kafka-sink/conf/cassandra-sink.properties`

## Mapping specification

See [the integration test](sink/src/it/java/com/datastax/oss/kafka/sink/simulacron/SimpleEndToEndSimulacronIT.java) for 
details.
