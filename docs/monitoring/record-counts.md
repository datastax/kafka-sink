---
title: Metrics for the processed Kafka topic records
source: monitoring/kafkaRecordCountMetrics.html
---

📘 [Documentation](../README.md) > [Monitoring](README.md) > Record Counts

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Metrics for the processed Kafka topic records
 
Running total and averages of the Kafka topic records processed by the DataStax Kafka
 Connector.
 
Use the Record Count MBeans to monitor the total number of Apache Kafka™
 records processed by the DataStax Connector.
 
> **Note:** Note: To capture these metrics, the [jmx](../config/connection.md#kafkaDseConnection__jmx)
 parameter must be set to `true` in the DataStax Apache Kafka®
 Connector configuration file.
 
**Count**
: Running total of records processed.
 
```
objectName='com.datastax.kafkaconnector:connector=connector_name,name=topic.keyspace.table.recordCount' attribute='Count'
```

 where 
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).

**MeanRate**
: Average number of records processed since the connector was created.
 
```
objectName='com.datastax.kafkaconnector:connector=connector_name,name=topic.keyspace.table.recordCount' attribute='MeanRate'
```
where 
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).

**OneMinuteRate**
: Moving average of records processed in the last
 minute.
```
objectName='com.datastax.kafkaconnector:connector=connector_name,name=topic.keyspace.table.recordCount' attribute='OneMinuteRate'
```
where 
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).

**FiveMinuteRate**
: Moving average of records processed in the last five
 minutes.
```
objectName='com.datastax.kafkaconnector:connector=connector_name,name=topic.keyspace.table.recordCount' attribute='FiveMinuteRate'

```
where 
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).

**FifteenMinuteRate**
: Moving average of records processed in the last fifteen
 minutes.
```
objectName='com.datastax.kafkaconnector:connector=connector_name,name=topic.keyspace.table.recordCount' attribute='FifteenMinuteRate'
```
where 
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).
