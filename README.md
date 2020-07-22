# DataStax Apache Kafka Connector

An Apache Kafka® sink for transferring events/messages from Kafka topics to Apache Cassandra®,
DataStax Astra or DataStax Enterprise (DSE).

## Installation

To download and install this connector please follow the procedure detailed [here](https://docs.datastax.com/en/kafka/doc/kafka/install/kafkaInstall.html).

## Documentation

All documentation is available online [here](https://docs.datastax.com/en/kafka/doc/index.html).

## Building from the sources

If you want to develop and test the connector you need to build the jar from sources.
To do so please follow those steps:

1. First build the uber-jar: 

       mvn clean package

2. Open the Connect worker config file `config/connect-standalone.properties`. Update the plugin 
   search path to include the uber-jar:

       plugin.path=<previous value>,<full path to repo>/dist/target/kafka-connect-cassandra-sink-<version>.jar

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

To see practical examples and usages of mapping, see:
https://docs.datastax.com/en/kafka/doc/search.html?searchQuery=mapping 
