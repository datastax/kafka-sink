---
title: DataStax Apache Kafka Connector configuration parameter reference
source: kafkaConfigToc.html
---

📘 [Documentation](../README.md) > Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](#) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# DataStax Apache Kafka Connector configuration parameter reference
 
Use the sample configuration files as a starting point to configure the DataStax Apache Kafka™ Connector. The connector supports a wide range of configuration options for connecting to your database, mapping topics to tables, and tuning performance.

## Sample Configuration Files

The DataStax Apache Kafka Connector distribution package includes sample configuration files in the `conf` directory. Use these as a starting point for your configuration:

- **[cassandra-sink-distributed.json.sample](../../dist/conf/cassandra-sink-distributed.json.sample)** - For distributed mode (JSON format with all settings enumerated and active)
- **[cassandra-sink-standalone.properties.sample](../../dist/conf/cassandra-sink-standalone.properties.sample)** - For standalone mode (Java properties file with descriptions and default values commented out)

You can also find these sample files in the [dist/conf](../../dist/conf) directory.

## Configuration Topics

### Core Configuration

- **[Connector details](connector.md)** - Common Kafka connector parameters including name, connector class, and tasks configuration
- **[Connection settings](connection.md)** - Configure the connection from Apache Kafka to the database cluster
- **[Topic-to-table mapping](table.md)** - Capture Kafka topics in the database by specifying target keyspace, table, and field mappings
- **[Configuring the connector](tasks.md)** - Overview of configuring the DataStax Apache Kafka Connector

### Authentication & Security

- **[Authentication](authentication.md)** - Provide credentials for internal authentication, LDAP, or Kerberos keytab file location
- **[Internal or LDAP authentication](ldap-auth.md)** - Configure username and password settings for internal or LDAP authentication
- **[Kerberos authentication](kerberos.md)** - Configure Kerberos settings when the cluster has Kerberos authentication enabled
- **[SSL encrypted connection](ssl.md)** - Configure SSL keys and certificates when the cluster has client encryption enabled

### Advanced Configuration

- **[Java driver settings](java-driver.md)** - Pass Kafka Connector settings directly to the DataStax Java driver using the `datastax-java-driver` prefix
- **[Date and time conversion](dates.md)** - Configure date and time conversion parameters for each topic
- **[Parallelism configuration](tasks-max.md)** - Adjust the number of tasks, simultaneous writes, and batch size for optimal performance
- **[Worker configuration](worker-config.md)** - Configure Kafka Connect Worker parameters to interact with Kafka Brokers in the cluster
- **[Logging configuration](logging.md)** - Configure logging for DataStax Apache Kafka Connector
- **[System requirements](sizing.md)** - System requirements that vary depending on workload and network capacity

## Related Documentation

- [Installation Guide](../install/README.md)
- [Operations Guide](../operations/README.md)
- [Mapping Topics to Tables](../mapping/README.md)
