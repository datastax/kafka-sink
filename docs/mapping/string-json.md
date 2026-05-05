---
title: Mapping a message that contains both basic and JSON fields
source: kafkaStringJson.html
---

📘 [Documentation](../README.md) > [Data Mapping](README.md) > String JSON

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping a message that contains both basic and JSON fields
 
When the data format for the Kafka key or value is JSON, individual fields of that
 JSON structure can be specified in the connector mapping.
 
When the data format for the message key or value is JSON, the connector mapping can
 include individual fields in the JSON structure. DataStax Apache Kafka™ supports JSON produced by both the `JsonSerializer`
 and `StringSerializer`; mapping semantics are the same.
 In the following example, the key is text field and the value is JSON. The key is
 mapped to the name field and each of the JSON fields to a separate column in the
 table.
| key | value |
| --- | --- |
| APPLE | ```json {"symbol":"APPL", "value":208, "exchange":"NASDAQ", "industry":"TECH", "ts":"2018-11-26T19:26:27.483"} ``` |
| EXXON MOBIL | ```json {"symbol":"M", "value":80, "exchange":"NYSE", "industry":"ENERGY", "ts":"2018-11-26T19:26:27.483"} ``` |
| GENERAL MOTORS | ```json {"symbol":"GM", "value":38, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |
| AT&T | ```json {"symbol":"AT&T", "value":33, "exchange":"NYSE", "industry":"TELECOM", "ts":"2018-11-26T19:26:27.483"} ``` |
| FORD MOTOR | ```json {"symbol":"F", "value":10, "exchange":"NYSE", "industry":"AUTO", "ts":"2018-11-26T19:26:27.483"} ``` |

> **Note:** Note: JSON records in Kafka can also have a schema associated with
 them.
 
Table requirements
 Ensure the following when mapping fields to columns:
- Data in the Kafka field is compatible with the database table column
 data type.
- Kafka field mapped to a database primary key (PK)
 column always contains data. Null values are not allowed in PK
 columns.
 
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
3. In the DataStax Connector configuration file: 
1. Add the topic name to [topics](../config/table.md#kafkaCassandraTable__topics).
2. Define the topic-to-table map [prefix](../config/table.md#kafkaCassandraTable__prefix).
3. Define the [field-to-column
 map](configuration_reference/kafkaDseTable.md#kafkaCassandraTable__mapping).
 Example configurations for `stocks_topic` to
 `stocks_table` using the minimum required settings: 
> **Note:** Note: See [DataStax Apache Kafka Connector configuration parameter reference](../monitoring/README.md) for additional
 parameters. When the [contactPoints](../config/connection.md#kafkaDseConnection__contactPoints) parameter is
 missing, the `localhost`; this assumes the database is co-located on
 the DataStax Apache Kafka Connector node.

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
 "symbol=value.symbol, ts=value.ts, exchange=value.exchange, industry=value.industry, name=key, value=value.value"
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
topic.stocks_topic.stocks_keyspace.stocks_table.mapping = symbol=value.symbol, ts=value.ts, exchange=value.exchange, industry=value.industry, name=key, value=value.value
```
4. [Update configuration on a running
 worker](operations/kafkaUpdateConfig.md) or [deploy the DataStax Connector
 for the first time](operations/kafkaStartStop.md).
