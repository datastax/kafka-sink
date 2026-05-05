📘 [Documentation](../README.md) > [Data Mapping](README.md) > Avro Data

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping Avro messages
 
Supports mapping individual fields from a Avro format field.
 
The DataStax Apache Kafka™ Connector supports mapping individual fields from
 [Avro](https://avro.apache.org/docs/1.8.1/gettingstartedjava.html) formatted key or values. 
 In this example, the key is a basic string and the value is Avro format. The Kafka topic name
 is `users_topic` and have the following records:
> **Note:** Note: The
 `kafka-avro-consumer` outputs the Avro field as JSON to the console. When
 Avro records are written to Kafka they are often done so using [GenericRecord](https://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/generic/GenericRecord.html) objects, which are fundamentally
 different than JSON.

| key | value |
| --- | --- |
| user0 | ```json {“name”: “chris”, “favorite_number”:14, “favorite_color”: “blue”} ``` |
| user1 | ```json {“name”: “jack”, “favorite_number”:56, “favorite_color”: “pink”} ``` |
| user2 | ```json {“name”: “shereen”, “favorite_number”:7, “favorite_color”: “black”} ``` |
| user3 | ```json {“name”: “kimberly”, “favorite_number”:11, “favorite_color”: “orange”} ``` |
| user4 | ```json {“name”: “taryn”, “favorite_number”:37, “favorite_color”: “green”} ``` |
 The value field uses the following Avro
 Schema:
```json
{
 "namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
 {"name": "name", "type": "string"},
 {"name": "favorite_number", "type": “int”},
 {"name": "favorite_color", "type": “string”}
 ]
}
```
 The DataStax keyspace name is `users_keyspace` and table is
 `users_table`. 
| userid | name | favoritenumber | favoritecolor |
| --- | --- | --- | --- |
| user0 | chris | 14 | blue |
| user1 | jack | 56 | pink |
| user2 | shereen | 7 | black |
| user3 | kimberly | 11 | orange |
| user4 | taryn | 37 | green |
 To create the table use the following
 command:
```
CREATE TABLE users_keyspace.users_table (userid text primary key, name text,
 favoritenumber int, favoritecolor text)
```
 
Configure the connector and use the following map specification:
 
```
“topic.users_topic.users_keyspace.users_table.mapping”: “userid=key,
 name=value.name, favoritenumber=value.favorite_number,
 favoritecolor=value.favorite_color”
```
See
 the [DataStax Kafka Examples](https://github.com/datastax/kafka-examples/tree/master/producers/src/main/java/avro) for more.
