---
title: Kafka-DataStax compatibility
source: kafkaCompatibility.html
---

📘 [Documentation](../README.md) > [Introduction](README.md) > Compatibility

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Kafka-DataStax compatibility
 
Kafka and DataStax platform compatibility matrix.
 The DataStax Apache Kafka™ Connector can stream data to:
- [DataStax Astra](https://docs.astra.datastax.com/docs) cloud databases
- DataStax Enterprise (DSE) 4.7 and later databases
- Open source Apache Cassandra® 2.1 and later databases

## Java Compatibility

| Connector Version | Java 8 | Java 11 | Java 17 | Java 21 |
|-------------------|--------|---------|---------|---------|
| 1.7.4+            | ✅     | ✅      | ✅      | ✅      |
| 1.7.3 and earlier | ✅     | ✅      | ❌      | ❌      |

Starting with version 1.7.4, the connector supports Java 17 and Java 21 in addition to Java 8 and Java 11.
 
## Supported Kafka data structures
 Ingest data from Kafka topics with records in the following data structures: 
- Primitive type values, such as integer or string
- Complex field values in record types:
- JSON formatted string
- Kafka Struct
- Avro
