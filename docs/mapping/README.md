📘 [Documentation](../README.md) > Data Mapping

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping kafka topics to database tables
 
Simple but powerful syntax for mapping Kafka fields to supported database table columns.
 
DataStax Apache Kafka™ Connector has a simple yet powerful syntax for mapping fields from a Kafka record to columns in a supported database table. An instance of the DataStax Apache Kafka Connector can capture multiple topics and supports mapping a single topic to one or more tables.
 
## Databases supported by DataStax Apache Kafka™ Connector

DataStax Apache Kafka Connector supports topic-to-table mappings with the following:

- [DataStax Astra](https://docs.astra.datastax.com/docs) cloud databases
- DataStax Enterprise (DSE) 4.7 and later databases
- Open source Apache Cassandra® 2.1 and later databases

## Topics

### Message Format Support

- **[Mapping basic messages to table columns](key-value-pairs.md)** - Create a topic-table map for Kafka messages that only contain a key and value in each record
- **[Mapping Avro messages](avro.md)** - Map individual fields from Avro format messages to database columns
- **[Mapping JSON messages](json-schema.md)** - Map JSON messages with or without a schema
- **[Mapping a message that contain JSON fields](json.md)** - Map individual fields in JSON structures to database columns
- **[Mapping a message that contains both basic and JSON fields](string-json.md)** - Handle messages with mixed basic and JSON field types
- **[Mapping a Kafka Struct](struct.md)** - Map records with a key and Apache Kafka™ Struct value

### Advanced Mapping Scenarios

- **[Mapping a topic to multiple tables](multiple-tables.md)** - Ingest a single topic into multiple tables using a single connector instance
- **[Multiple topics to multiple tables](multiple-topics.md)** - Ingest multiple topics and write to different tables using a single connector instance
- **[Extract Kafka record header values](record-headers.md)** - Extract values from Kafka record headers and write to the database table
- **[Selectively update maps and UDTs based on Kafka fields](selective-updates.md)** - Selectively update maps and User Defined Types (UDTs) based on present Kafka fields

### Configuration and Processing

- **[How Apache Kafka messages are written](message-processing.md)** - Overview of the Apache Kafka™ topic data pipeline
- **[Configuring the worker to convert serialized bytes](producer-apis.md)** - Configure the worker to deserialize messages using the converter that corresponds to the producer's serializer

## Related Documentation

- [Configuration Reference](../config/README.md)
- [Operations Guide](../operations/README.md)
- [Introduction](../intro/README.md)
