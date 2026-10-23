📘 [Documentation](../README.md) > [Configuration](README.md) > Connection Settings

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# DataStax connection
 
Configure the connection to the cluster.
 
Configure the connection settings from Apache Kafka™ to the cluster. 
 
## Parameters
 
```
connectionPoolLocalSize=4
contactPoints=[dse_host_list]
loadBalancing.localDc=datacenter_name
#port=9042
#maxConcurrentRequests=500
#maxNumberOfRecordsInBatch=32
#queryExecutionTimeout=30
#jmx=true
#compression=None
```
 
**connectionPoolLocalSize**
: Number of connections that driver maintains within a connection pool to each node in
 the local datacenter.
Default: `4`

**contactPoints**
: A comma-separated list of host names or IP addresses in square
 brackets.
> **Note:** Note: When this setting is specified (for example, the database is on a
 remote host), the [loadBalancing.localDc](../config/connection.md#kafkaDseConnection__loadBalancing_localDc) is
 required.

Default: `localhost`

**loadBalancing.localDc**
: The case-sensitive datacenter name for the driver to use for load balancing.
> **Note:** Note: You cannot use this option if specifying the [cloud.secureConnectBundle](../config/connector.md#kafkaConnector__secure_ConnectBundle) option for
 connecting to a DataStax Astra database.

Default: `""`

**port**
: DSE native transport port. See native_transport_port.
 
Default: `9042`

**maxConcurrentRequests**
: Maximum number of requests to send to DSE at the same
 time.
Default: `500`

**maxNumberOfRecordsInBatch**
: Number of records to include in a write request to the database
 table.
Default: `32`

**jmx**
: Whether to enable metrics reporting using Java Management Extensions (JMX). See [Monitoring DataStax Apache Kafka Connector](../monitoring/README.md).
Default: `true`

**compression**
: Compression algorithm to use when issuing requests to the database server. Valid values are `None`,
 `Snappy`, and `LZ4`, which is applied per connector
 instance.
Default: `None`

**queryExecutionTimeout**
: CQL statement execution timeout, in seconds. 
Default: `30`
