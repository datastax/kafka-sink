📘 [Documentation](README.md) > FAQ

---

**Quick Links:** [Home](README.md) | [Install](install/README.md) | [Config](config/README.md) | [Operations](operations/README.md) | [Monitoring](monitoring/README.md) | [Troubleshooting](troubleshooting/README.md)

---

# Frequently Asked Questions (FAQ)

## Compatibility

### What versions of Java are supported?

| Connector Version | Java 8 | Java 11 | Java 17 | Java 21 |
|-------------------|--------|---------|---------|---------|
| 1.7.4+            | ✅     | ✅      | ✅      | ✅      |
| 1.7.3 and earlier | ✅     | ✅      | ❌      | ❌      |

Starting with version 1.7.4, the connector supports Java 17 and Java 21 in addition to Java 8 and Java 11.

### What versions of Kafka are supported?

The DataStax Apache Kafka Connector is compatible with:
- Apache Kafka 2.x and 3.x
- Confluent Platform 5.x, 6.x, and 7.x

For detailed compatibility information, see the [Compatibility Guide](intro/compatibility.md).

### What databases are supported?

The connector can stream data to:
- [DataStax Astra](https://docs.astra.datastax.com/docs) cloud databases
- DataStax Enterprise (DSE) 4.7 and later
- Apache Cassandra® 2.1 and later

### What data formats are supported?

The connector supports:
- Primitive types (integer, string, etc.)
- JSON formatted strings
- Kafka Struct
- Avro

See the [Data Mapping Guide](mapping/README.md) for details.

## Installation

### How do I install the connector?

See the [Installation Guide](install/README.md) for detailed instructions on installing the connector.

### Can I use the connector with Confluent Platform?

Yes, the connector is compatible with Confluent Platform. See the [Installation Guide](install/README.md) for Confluent-specific instructions.

## Configuration

### How do I configure SSL/TLS connections?

See the [SSL Configuration Guide](security/ssl.md) for detailed SSL/TLS setup instructions.

### How do I configure authentication?

The connector supports multiple authentication methods:
- Username/password authentication - see [Authentication Guide](config/authentication.md)
- Kerberos - see [Kerberos Configuration](security/kerberos-config.md)
- LDAP - see [LDAP Authentication](security/ldap-auth.md)

### How many tasks should I configure?

The number of tasks affects parallelism and throughput. See the [Tasks Configuration Guide](config/tasks-max.md) for guidance on setting the optimal number of tasks.

## Data Mapping

### Can I map one topic to multiple tables?

Yes! See the [Multiple Tables Guide](mapping/multiple-tables.md) for details on routing data from a single topic to multiple tables.

### Can I map multiple topics to one table?

Yes! See the [Multiple Topics Guide](mapping/multiple-topics.md) for details on routing data from multiple topics to a single table.

### How do I handle JSON data?

The connector supports multiple JSON formats:
- [JSON strings](mapping/json.md)
- [JSON with schema](mapping/json-schema.md)

### How do I set TTL on records?

See the [Row-Level TTL Guide](operations/row-level-ttl.md) for instructions on setting time-to-live values for inserted records.

## Operations

### How do I monitor the connector?

The connector provides comprehensive JMX metrics. See the [Monitoring Guide](monitoring/README.md) for details on:
- [Enabling JMX](monitoring/jmx.md)
- [DataStax-specific metrics](monitoring/datastax-metrics.md)
- [Kafka Connect metrics](monitoring/connect-metrics.md)
- [Write metrics](monitoring/write-metrics.md)

### How do I scale the connector?

See the [Scaling Guide](operations/scaling.md) for information on scaling the connector for higher throughput.

### What CQL queries does the connector use?

See the [CQL Queries Guide](operations/cql-queries.md) for details on the CQL statements used by the connector.

## Troubleshooting

### Where can I find troubleshooting information?

See the [Troubleshooting Guide](troubleshooting/README.md) for common issues and solutions.

### How do I handle failed records?

See the [Failed Records Monitoring](monitoring/failed-records.md) for information on tracking and handling failed records.

## Additional Resources

- [Release Notes](release-notes/README.md) - Version history and changes
- [Tutorials](tutorials/README.md) - Step-by-step guides
- [Architecture Overview](intro/architecture.md) - Understanding the connector architecture