# Apache Kafka Sink for Apache Cassandra and DataStax Enterprise

An Apache Kafka sink for transferring events/messages from Kafka topics to Apache Cassandra (R) or
DataStax Enterprise (DSE).

## How to use

1. First build the uber-jar: 

       mvn clean package

2. Open the Connect worker config file `config/connect-standalone.properties`. Update the plugin 
   search path to include the uber-jar:

       plugin.path=<previous value>,<full path to repo>/dist/target/kafka-connect-cassandra-<version>.jar

3. Edit the `dist/conf/cassandra-sink-standalone.properties.sample` config file in this project to 
   meet your needs, or copy it out and edit elsewhere. The edited file should be named 
   `cassandra-sink-standalone.properties`.

4. Run connect-standalone and specify the path to the that config file:

       bin/connect-standalone.sh \
          config/connect-standalone.properties 
          <full path to file>/cassandra-sink-standalone.properties

5. In Confluent, you would do this:

       bin/confluent load cassandra-sink -d <full path to file>/cassandra-sink-standalone.properties

## Mapping specification

See [the integration test](sink/src/it/java/com/datastax/oss/kafka/sink/simulacron/SimpleEndToEndSimulacronIT.java) for 
details.
