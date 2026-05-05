📘 [Documentation](../README.md) > [Configuration](README.md) > Parallelism Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Configuring parallelism
 
Adjusting the number of tasks, simultaneous writes, and batch size.
 
In order for workers to split DataStax Apache Kafka™ connector tasks, the
 workers must have the same `group.id` in the
 `connect-distributed.properties` file. The parallelism is limited by the
 partitions for the given Kafka topic. 
 Example - Kafka topic named `my-topic` has 10 partitions and the connector
 group is configured to have [20 tasks](../config/connector.md#kafkaConnector__tasks_max). The
 maximum unit of parallelism in this environment would be 10 because each partitions cannot be
 subdivided among the tasks. 
> **Note:** Note: Use additional Apache Kafka™ Connect workers
 to scale the connector if greater throughput is needed.
