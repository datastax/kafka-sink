📘 [Documentation](../README.md) > [Data Mapping](README.md) > Struct Data

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping a Kafka Struct
 
Mapping a record with a key and Apache Kafka™ Struct value.
 A `Struct` is an Apache Kafka™ Connect data object that is
 used to represent complex data structures. Process a `Struct` with either the
 `JsonConverter` or `AvroConverter`. Typically, Kafka Source
 Connectors use `Struct` when writing records to Kafka. Specify individual
 fields of the `Struct` in the connector mapping. 
> **Note:** Tip: See the
 Apache Kafka javadocs for more information. 
 
In this example, `baseball_topic` has a primitive string key and
 JSON `Struct` value. The DataStax Connector can process the individual fields
 of the `Struct`. 
 Sample Kafka Record
| key | value |
| --- | --- |
| redsox | ```json {"schema": {"type": "struct", "fields": [{"type":"int32","optional":true,"field":"personid"}, {"type":"string","optional":true,"field":"firstname"}, {"type":"string","optional":true,"field":"lastname"}, {"type":"string","optional":true,"field":"street"}, {"type":"string","optional":true,"field":"city"}], "optional":false,"name":"addresses"}, "payload": { "number":50, "firstname":"mookie", "lastname":"betts", "street":"4 yawkey way", "city":"boston"}} ``` |
 The DataStax keyspace name is `baseball_keyspace` and table name is
 `baseball_table`. Use the following command to create the
 table:
```cql
CREATE TABLE baseball_keyspace.baseball_table (team text primary key, number int,
 firstname text, lastname text, street text, city text);
```
 Configure the connector settings and use the following to map the
 fields:
```
“topic.baseball_topic.baseball_keyspace.baseball_table.mapping”: 
“team=key,number=value.number, firstname=value.firstname, lastname=value.lastname, street=value.street, city=value.city”
```
 
See the [DataStax Kafka Examples](https://github.com/datastax/kafka-examples/tree/master/connectors/jdbc-source-connector) for more.
