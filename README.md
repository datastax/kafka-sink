# DataStax Apache Kafka Connector

An Apache Kafka® sink for transferring events/messages from Kafka topics to Apache Cassandra®,
DataStax Astra or DataStax Enterprise (DSE).

## Documentation

Full documentation is available in the [`docs/`](docs/README.md) directory.

### Quick Links

- **[Documentation Home](docs/README.md)** - Start here for complete documentation
- **[Introduction and Overview](docs/intro/README.md)** - Learn about the connector architecture and features
- **[Installation Guide](docs/install/README.md)** - Install and configure the connector
- **[Configuration Reference](docs/config/README.md)** - Complete configuration parameter reference
- **[Data Mapping](docs/mapping/README.md)** - Map Kafka topics to database tables
- **[Operations Guide](docs/operations/README.md)** - Operational procedures and best practices
- **[Security](docs/security/README.md)** - Security configuration (SSL, Kerberos, LDAP)
- **[Monitoring](docs/monitoring/README.md)** - Metrics and monitoring with JMX
- **[Troubleshooting](docs/troubleshooting/README.md)** - Common issues and solutions
- **[Tutorials](docs/tutorials/README.md)** - Step-by-step guides
- **[Release Notes](docs/release-notes/README.md)** - Version history and changes

## Installation

See the **[Installation Guide](docs/install/README.md)** for detailed installation instructions.

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

See the **[Data Mapping Guide](docs/mapping/README.md)** for complete mapping documentation.

## FAQ

For frequently asked questions including Java/Kafka compatibility, installation, configuration, and more, see the **[FAQ](docs/faq.md)**.
