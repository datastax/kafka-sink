---
title: DataStax Apache Kafka Connector details
source: configuration_reference/kafkaConnector.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > Connector Settings

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# DataStax Apache Kafka Connector details
 
Common Kafka connector parameters.
 
Set the common DataStax Apache Kafka™ Connector parameters. See the [Configuring Connectors](https://kafka.apache.org/documentation/#connect_configuring) section of the Apache Kafka
 documentation.
 
## Parameters
 
```
name=dse-sink
connector.class=com.datastax.kafkaconnector.DseSinkConnector
tasks.max=1
cloud.secureConnectBundle=/path/to/secure-connect-database_name.zip
```
 
**name**
: Unique name for the connector. 
Default: `cassandra-sink`

**connector.class**
: DataStax connector Java class provided in the
 `kafka-connect-cassandra-sink-N.N.N.jar`
Default: `com.datastax.kafkaconnector.CassandraSinkConnector`

**tasks.max**
: Maximum number of tasks created for the connector instance. The connector can create
 fewer tasks if it cannot achieve this level of parallelism. 
> **Note:** Note: Use additional Kafka
 Connect workers to scale the connector when more throughput is needed. For connector
 instances to split tasks, they must have the same `group.id`, which is
 configured in the connect-distributed.properties file.
 Parallelism is limited by the partitions of the Kafka topic.
 

Default: `1`

**cloud.secureConnectBundle**
: The full path to the secure connect bundle for your DataStax Astra database
 (secure-connect-database_name.zip). Download
 the secure connect bundle from the
 DataStax Cloud
 console.
If this option is specified, you must also include the [auth.username](../config/ldap-auth.md#kafkaAuthLdap__username) and [auth.password](../config/ldap-auth.md#kafkaAuthLdap__password) for the database user.
