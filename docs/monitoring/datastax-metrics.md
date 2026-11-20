📘 [Documentation](../README.md) > [Monitoring](README.md) > DataStax Metrics

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# DataStax Kafka Connector metrics
 
Use JMX to monitor the DataStax Kafka Connector.
 
Metrics for requests sent by the DataStax Apache Kafka™ Connector instance
 are written to Java Management Extension MBeans under the namespace:
 `com.datastax.kafkaconnector`.
 
> [!NOTE]
> To capture these metrics, the [jmx](../config/connection.md#kafkaDseConnection__jmx) parameter must be set to `true` in the DataStax Apache Kafka® Connector configuration file.
