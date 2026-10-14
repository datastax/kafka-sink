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

## Optional RecordListener

Callbacks are off unless `record.listener.class` is set. A `META-INF/services` provider is not loaded automatically. Set `record.listener.path` when the implementation is not already on the plugin classloader.

| Key | Default | Meaning |
|-----|---------|---------|
| `record.listener.class` | unset | Fully qualified `com.datastax.oss.kafka.sink.spi.RecordListener` |
| `record.listener.path` | unset | Comma-separated jars and/or directories loaded in a child classloader |

`onSuccess` runs after Cassandra accepts the write. `onFailure` runs after a mapping or CQL failure (`cql` is non-null on the driver path). Errors while loading the listener or running a callback are logged and do not fail `put`.

## Mapping specification

To see practical examples and usages of mapping, see:
https://docs.datastax.com/en/kafka/doc/search.html?searchQuery=mapping 

## Java Compatibility
| Connector Version | Java 8 | Java 11 | Java 17 | Java 21 |
|-------------------|--------|---------|---------|---------|
| 1.7.4+            | ✅     | ✅      | ✅      | ✅      |
| 1.7.3 and earlier | ✅     | ✅      | ❌      | ❌      |
