---
title: Monitoring DataStax Apache Kafka Connector
source: monitoring/kafkaMetrics.html
---

📘 [Documentation](../README.md) > Monitoring

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Monitoring DataStax Apache Kafka Connector
 
Use metrics reported for both the Kafka Connect Workers and the DataStax Apache Kafka Connector by using Java Management Extension MBeans to monitor the connector.
 
Use metrics reported for the Kafka Connect Workers and the DataStax Apache Kafka™ Connector via Java Management Extension MBeans to monitor the connector.

## Topics

### Setup and Configuration

- **[Enabling Java Management Extension remote connections](jmx.md)** - Allow remote JMX connections to monitor DataStax Apache Kafka Connector activity

### Metrics Categories

- **[Kafka Connect metrics](connect-metrics.md)** - Use the Apache Kafka™ Connect Framework Java Management Extensions (JMX) metrics to monitor the DataStax Apache Kafka Connector consumption of topics
- **[DataStax Kafka Connector metrics](datastax-metrics.md)** - Metrics for requests sent by the DataStax Apache Kafka™ Connector instance
- **[Metrics for the processed Kafka topic records](record-counts.md)** - Running total and averages of the Kafka topic records processed by the DataStax Kafka Connector
- **[Failed Kafka topic record metrics](failed-records.md)** - Running total or moving averages of Kafka topic records that could not be processed by a DataStax Apache Kafka Connector instance
- **[DataStax Apache Kafka Connector - Batch size metrics](write-metrics.md)** - Write statistics for each mapping of a Kafka topic to a database table

## Related Documentation

- [Configuration Reference](../config/README.md)
- [Operations Guide](../operations/README.md)
- [Troubleshooting](../troubleshooting/README.md)
