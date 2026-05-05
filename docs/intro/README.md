📘 [Documentation](../README.md) > Introduction

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# About the DataStax Apache Kafka Connector
 
The DataStax Apache Kafka™ Connector is open-source software (OSS) installed in the Kafka Connect framework, and synchronizes records from a Kafka topic with table rows in the following supported databases:

- [DataStax Astra](https://docs.astra.datastax.com/docs) cloud databases
- DataStax Enterprise (DSE) 4.7 and later databases
- Open source Apache Cassandra® 2.1 and later databases

## Topics

- **[Understanding the architecture](architecture.md)** - Components of a DataStax Apache Kafka Connector implementation
- **[How data is written to the target platform](about-instance.md)** - Data from the Kafka topic is written to the mapped platform's database table using batch requests
- **[Features](features.md)** - Key features in DataStax Apache Kafka Connector
- **[Kafka-DataStax compatibility](compatibility.md)** - Kafka and DataStax platform compatibility matrix

## Related Documentation

- [Installation Guide](../install/README.md)
- [Configuration Reference](../config/README.md)
- [Mapping Topics to Tables](../mapping/README.md)
