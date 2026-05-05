---
title: Worker configuration parameters
source: kafkaWorkerConfig.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > Worker Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Worker configuration parameters
 
Configure Kafka Connect Worker parameters so it can interact with Kafka Brokers in the cluster.
 
**bootstrap.servers**
: Initial hosts that act as the starting point for the Kafka Connect Worker to
 discover the full set of alive Kafka Brokers in the Kafka cluster.

**key.converter**
: The name of the converter class used to deserialize Kafka message keys. Use
 the converter that corresponds to the serializers used by Producers.

**value.converter**
: The name of the converter class used to deserialize Kafka message values.
 Use the converter that corresponds to the serializers used by
 Producers.

**value.converter.schemas.enable**
: Enables (`true`) or disables (`false`)
 converter schemas.
Default: `false`.

**group.id**
: For distributed mode only. Unique name for the cluster, used in forming the
 Connect cluster group.
> **Note:** Note: Must not conflict with consumer group
 IDs.

**plugin.path**
: Set to a list of filesystem paths separated by commas (,). To enable class
 loading isolation for plugins (connectors, converters, and transformations).
 The list can include any combination of: 
- Directories that contain jars with plugins and their
 dependencies
- Uber-jars with plugins and their dependencies

> **Note:** Note: When symlinks are specified, Kafka follows the link to discover
 dependencies or plugins.

For example, to look for
 plugins in two locations:
 `plugin.path=,/usr/local/share/kafka/plugins,/opt/connectors`.
