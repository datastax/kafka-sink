---
title: Configuring the worker to convert serialized bytes
source: kafkaProducerApis.html
---

📘 [Documentation](../README.md) > [Data Mapping](README.md) > Producer APIs

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Configuring the worker to convert serialized bytes
 
Configure the worker to deserialize messages using the converter that corresponds to
 the producer's serializer.
 
Configure the worker that runs DataStax Apache Kafka™ Connector to
 deserialize Kafka messages using the correct converter. Typically, all producers and streams
 use the same serialization definition to write messages.
 
Apache Kafka provides several APIs for publishing data. Each API provides several different
 options for message structure and serialization. 
 Apache Kafka APIs for publishing data include:
- Kafka Producer API. See [Apache Kafka Java Doc for the Producer
 Classes](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html).
- Kafka Source Connectors. See [Apache Kafka Documentation on the Connect API](https://kafka.apache.org/documentation.html#connectapi).
- Kafka Streams. See [Apache Kafka Documentation on the Streams API](https://kafka.apache.org/documentation.html#streamsapi) / KSQL.
