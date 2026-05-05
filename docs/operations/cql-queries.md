---
title: Provide CQL queries in mappings
source: kafkaCqlQuery.html
---

📘 [Documentation](../README.md) > [Operations](README.md) > CQL Queries

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Provide CQL queries in mappings
 
Provide CQL queries when new record arrives on the Kafka topic.
 
In the Kafka topic mapping, you can optionally provide a CQL query that should run when the
 new record arrives on the Kafka topic. 
 
> **Note:** Note: This feature is only meant for advanced use cases. Most often, you can use the standard
 Kafka mapping without a query. If you define a query in the topic-to-table mapping, the
 query has priority and will be used instead of the automatically generated action. 
 If used, you must provide the bound variables used in the query in the mapping column. For
 example, give the following query:
 
```sql
topic.my_topic.my_ks.my_table.query=INSERT INTO %s.types (bigintCol, intCol) VALUES (:some_name, :some_name_2)
```
The
 corresponding mapping in this example should be defined as:
 
```
topic.my_topic.my_ks.my_table.mapping=some_name=value.bigint, some_name_2=value.int
```

 Additional examples of CQL queries that you could include with the topic-to-table mapping:
 
```sql
topic.my_topic.my_ks.my_table.query=INSERT INTO ks.tbl(pkey,ccol,x) VALUES :pkey, :ccol, :x);
topic.my_topic.my_ks.my_table.query=INSERT INTO ks.tbl(pkey,ccol,x) VALUES (:pkey, :ccol, :x) USING TTL :input_ttl;
topic.my_topic.my_ks.my_table.query=INSERT INTO ks.tbl(pkey,ccol,x) VALUES (:pkey, :ccol, :x) USING TIMESTAMP :input_ts;
topic.my_topic.my_ks.my_table.query=UPDATE ks.tbl SET somelist = somelist + [:newitem] WHERE pkey = :pkey;
topic.my_topic.my_ks.my_table.query=DELETE FROM ks.tbl WHERE pkey = :pkey AND ccol = :ccol;
topic.my_topic.my_ks.my_table.query=DELETE FROM ks.tbl WHERE pkey = :pkey AND ccol > :ccol1 AND ccol <= :ccol2;
```
