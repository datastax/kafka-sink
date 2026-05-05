---
title: Mapping JSON messages
source: kafkaJsonMessageSchema.html
---

📘 [Documentation](../README.md) > [Data Mapping](README.md) > JSON Schema

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping JSON messages
 
Supports mapping JSON messages with or without a schema.
 
The DataStax Apache Kafka™ Connector supports mapping JSON
 messages with or without a schema. In this example, the key is regular JSON without
 schema. The value is also JSON but contains a schema and a payload. The type of the
 payload is Map and the connector is able to access the individual fields of that
 map. 

| **key** | **value** |
| {"name":"APPLE"} | {"schema":{"type":"map","fields":[{"type":"string","optional":false,"field":"symbol"},{"type":"int32","optional":true,"field":"value"},{"type":"string","optional":true,"field":"exchange"},{"type":"string","optional":true,"field":"industry"},{"type":"string","optional":false,"field":"ts"}],"optional":false,"name":"stocksdata"},"payload":{"symbol":"APPL","value":208,"exchange":"NASDAQ","industry":"TECH","ts":"2018-11-26T19:26:27.483"}} |
| {"name":"EXXON MOBIL"} | {"schema":{"type":"map","fields":[{"type":"string","optional":false,"field":"symbol"},{"type":"int32","optional":true,"field":"value"},{"type":"string","optional":true,"field":"exchange"},{"type":"string","optional":true,"field":"industry"},{"type":"string","optional":false,"field":"ts"}],"optional":false,"name":"stocksdata"},"payload":{"symbol":"M","value":80,"exchange":"NYSE","industry":"ENERGY","ts":"2018-11-26T19:26:27.483"}} |
| {"name":"GENERAL MOTORS”} | {"schema":{"type":"map","fields":[{"type":"string","optional":false,"field":"symbol"},{"type":"int32","optional":true,"field":"value"},{"type":"string","optional":true,"field":"exchange"},{"type":"string","optional":true,"field":"industry"},{"type":"string","optional":false,"field":"ts"}],"optional":false,"name":"stocksdata"},"payload":{"symbol":"GM","value":38,"exchange":"NYSE","industry":"AUTO","ts":"2018-11-26T19:26:27.483"}} |
| {"name":"AT&T”} | {"schema":{"type":"map","fields":[{"type":"string","optional":false,"field":"symbol"},{"type":"int32","optional":true,"field":"value"},{"type":"string","optional":true,"field":"exchange"},{"type":"string","optional":true,"field":"industry"},{"type":"string","optional":false,"field":"ts"}],"optional":false,"name":"stocksdata"},"payload":{"symbol":"AT&T","value":33,"exchange":"NYSE","industry":"TELECOM","ts":"2018-11-26T19:26:27.483"}} |
| {"name":"FORD MOTOR”} | {"schema":{"type":"map","fields":[{"type":"string","optional":false,"field":"symbol"},{"type":"int32","optional":true,"field":"value"},{"type":"string","optional":true,"field":"exchange"},{"type":"string","optional":true,"field":"industry"},{"type":"string","optional":false,"field":"ts"}],"optional":false,"name":"stocksdata"},"payload":{"symbol":"F","value":10,"exchange":"NYSE","industry":"AUTO","ts":"2018-11-26T19:26:27.483"}} |
See the [DataStax Kafka Examples](https://github.com/datastax/kafka-examples/tree/master/producers/src/main/java/json) for a full
 example. 
## Procedure

1. Verify that the correct converter is set in the [key.converter](../config/worker-config.md#key_converter) and [value.converter](../config/worker-config.md#value_converter) of the
 `connect-distributed.properties` file.
2. Set up the [supported database](../README.md#kafkaIntro__kafkaIntroduction) table. 
1. Create the keyspace. Ensure
 that keyspace is replicated to a datacenter that is set in the
 DataStax Apache Kafka Connector [contactPoints](../config/connection.md#kafkaDseConnection__contactPoints)
 parameter. For example, create the
 `stocks_keyspace`:
```bash
cqlsh -e "CREATE KEYSPACE stocks_keyspace \
WITH replication = {'class': 'NetworkTopologyStrategy',\
'Cassandra': 1};"
```

> **Note:** Note: The datacenter name is case sensitive. Use nodetool ring to get a list of
 datacenters.
2. Create the table. For example, create the
 `stocks_table`:
```bash
cqlsh -e "CREATE TABLE stocks_keyspace.stocks_table ( \
 symbol text, \
 ts timestamp, \
 exchange text, \
 industry text, \
 name text, \
 value double, \
 PRIMARY KEY (symbol, ts));"
```
3. Verify that all nodes have the same schema version using nodetool describering. Replace
 keyspace_name:
```bash
nodetool describering -- keyspace_name
```
3. In the DataStax Apache Kafka Connector configuration file: 
1. Add the topic name to [topics](../config/table.md#kafkaCassandraTable__topics).
2. Define the topic-to-table map [prefix](../config/table.md#kafkaCassandraTable__prefix).
3. Define the [field-to-column
 map](configuration_reference/kafkaDseTable.md#kafkaCassandraTable__mapping).
 Example configurations for `stocks_topic` to
 `stocks_table` using the minimum required settings: 
- JSON for distributed
 mode:
```json
{
 "name": "stocks-sink",
 "config": {
 "connector.class": "com.datastax.kafkaconnector.DseSinkConnector",
 "tasks.max": "1",
 "topics": "stocks_topic",
 "topic.stocks_topic.stocks_keyspace.stocks_table.mapping": 
 “symbol=value.symbol, ts=value.dateTime, exchange=value.exchange, industry=value.industry, name=key.name, value=value.value”
 }
}
```
- Properties file for standalone
 mode:
```
name=stocks-sink
connector.class=com.datastax.kafkaconnector.DseSinkConnector
tasks.max=1
topics=stocks_topic
topic.stocks_topic.stocks_keyspace.stocks_table.mapping = symbol=value.symbol,ts=value.dateTime,exchange=value.exchange,industry=value.industry,name=key.name,value=value.value
```

> **Note:** Note: See [DataStax Apache Kafka Connector configuration parameter reference](../monitoring/README.md) for additional
 parameters. When the [contactPoints](../config/connection.md#kafkaDseConnection__contactPoints) parameter is
 missing, the `localhost`; this assumes the database is co-located on
 the DataStax Apache Kafka Connector node.
4. [Update configuration on a running
 worker](operations/kafkaUpdateConfig.md) or [deploy the DataStax Connector
 for the first time](operations/kafkaStartStop.md).
