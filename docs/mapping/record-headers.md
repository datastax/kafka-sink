📘 [Documentation](../README.md) > [Data Mapping](README.md) > Record Headers

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Extract Kafka record header values
 
Extract values from Kafka record header and write to the database table.
 In the Kafka topic mapping, you can extract values from the record's header by using
 `header.header-field-name`, and write the values to a
 supported database table:
- [DataStax Astra](https://docs.astra.datastax.com/docs) cloud databases
- DataStax Enterprise (DSE) 4.7 and later databases
- Open source Apache Cassandra® 2.1 and later databases
 For example:
 
```
topic.my_topic.my_keyspace.my_table.mapping=col1=key.f1, col2=value.f1, __ttl=value.f2, __timestamp=value.f3, 
 col3=header.f4
```
 
You cannot map the entire header. Use one or more individual mapping statements to extract
 the desired values and have them written to the database table. The Kafka record's header can
 have fields that are similar to `key.field-name` or
 `value.field-name`. 
 
For example, you may want to send the Kafka record with its key, value, and custom header
 information such as `trace_id` or another type of internal identifier, and save
 those values in your database table.
