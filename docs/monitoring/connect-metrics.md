---
title: Kafka Connect metrics
source: monitoring/kafkaConnectMetrics.html
---

📘 [Documentation](../README.md) > [Monitoring](README.md) > Connect Metrics

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Kafka Connect metrics
 
Use the Apache Kafka™ Connect Framework Java Management Extensions (JMX)
 metrics to monitor the DataStax Apache Kafka Connector consumption of topics. 
 
> **Note:** Tip: This section covers a limited subset of the available JMX metrics. See [Apache Kafka Connect monitoring](https://kafka.apache.org/documentation/#connect_monitoring) for a detailed reference.
 
## Kafka Connect - Consumer metrics
 
Consumer metrics relevant for monitoring the DataStax Kafka
 Connector.
 Monitor the rate at which messages are pulled by the worker running
 the DataStax Apache Kafka™ Connector.
> **Note:** Tip: See Apache Kafka [consumer metrics](https://kafka.apache.org/documentation/#new_consumer_monitoring) documentation.
 
### Fetch manager metrics
 Use the fetch manager MBean to monitor the rate messages are pulled from the
 topic:
```
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=([-.w]+)
```
 Attributes:
**records-lag**
: Number of records for any partition in this window. An increasing value over time is
 a good indication that the consumer group is not keeping up with the
 producers.
```
objectName='kafka.consumer:type=consumer-fetch-manager-metrics,client-id=id' attribute='records-lag-max'
```
where the id is typically a number assigned
 to the worker by the Kafka Connect.

**records-consumed-rate**
: The average number of records consumed per
 second.
```
objectName='kafka.consumer:type=consumer-fetch-manager-metrics,client-id=datastax_connector_id' attribute=records-consumed-rate'
```
where the id is typically a number assigned
 to the worker by the Kafka Connect.
 
### Stream metrics
 Monitor the attribute values of the Connection Stream
 MBean:
```
kafka.consumer:type=consumer-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
```
 Attributes:
**incoming-byte-rate**
: Bytes per second read from all sockets by the
 worker.
```
objectName='kafka.consumer:type=consumer-fetch-manager-metrics,client-id=datastax_connector_id' attribute='incoming-byte-rate'
```
where the id is typically a number assigned
 to the worker by the Kafka Connect.
 
## Kafka Connect - Connector metrics
 
Number of records accessed across the number of partitions by
 the DataStax Apache Kafka Connector.
 Monitor the number of records pulled by each DataStax Apache Kafka™
 Connector task.
> **Note:** Tip: [tasks.max](../config/connector.md#kafkaConnector__tasks_max) sets the maximum
 number of tasks the DataStax Connector can run.
 
### Sink Task metrics
 Use the Kafka Connect Sink Task metrics to get information on the number of Kafka records
 and
 partitions.
```
kafka.connect:type=sink-task-metrics,connector="connector_name",task="*"

```
Use
 an asterisk to display all tasks or specify a task number.. 
**sink-record-read-total**
: The total number of records polled by this task, measured since the task was last
 restarted.
 
```
objectName='kafka.connect:type=sink-task-metrics,connector=*,task=*' attribute='sink-record-read-total'
```

**partition-count**
: The number of topic partitions assigned to this task. Shows how the partitions are
 balance amongst the
 tasks.
```
objectName='kafka.connect:type=sink-task-metrics,connector=*,task=*' attribute='partition-count'
```
