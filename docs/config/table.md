---
title: Kafka topic-to-table settings
source: configuration_reference/kafkaDseTable.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > Topic-to-Table Mapping

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Kafka topic-to-table settings
 
Capture Kafka topics in the supported database by specifying a
 target keyspace, table, and then map the topic fields to table columns. 
 
Capture Kafka topics in the [supported database](../README.md#kafkaIntro__kafkaIntroduction) by specifying a target keyspace, table, and
 then map the topic fields to table columns in the name of the parameter. 
 
## Parameters
 Use the following syntax for standalone properties
 file:
```
topics=topic_list
prefix.mapping=mapping_specification
prefix.consistencyLevel=WRITE_CONSISTENCY_LEVEL
prefix.ttl=seconds
prefix.nullToUnset=true
prefix.deletesEnabled=true
```
 
**topics**
: A comma separated list of all topics to which the DataStax Connector subscribes.

**prefix**
: Defines the topic-to-table to which the parameters apply. This allows records from
 a single topic to be ingested into multiple database tables. Define the
 parameter prefix using the following syntax:
 
```
topic.topic_name.keyspace_name.table_name
```

 where: 
- topic_name - Kafka topic name.
- keyspace_name - database
 keyspace where the table is located.
- table_name - database table
 where data is written.

**mapping**
: Required, field-to-column mapping. See [Mapping kafka topics to database tables](../monitoring/README.md).

**consistencyLevel**
: Query consistency level. DSE settings are:
- ALL
- EACH QUORUM
- QUORUM
- LOCAL_QUORUM
- ONE
- TWO
- THREE
- LOCAL_ONE (default)
- ANY
See [How are consistent read and write operations
 handled?](/en/dse/6.7/dse-arch/datastax_enterprise/dbInternals/dbIntAboutDataConsistency.md)
Default: `LOCAL_ONE`

**nullToUnset**
: Whether to treat nulls in Kafka as UNSET in DSE. DataStax recommends using the
 default to avoid creating unnecessary
 tombstones.
Default: `true`

**ttl**
: Time-to-live. Set to the number of seconds before the data is automatically deleted
 from the DSE table. When you configure
 `topic.<topic-name>.<keyspace-name>.<table-name>.ttl`, all
 rows for that topic table will have this same `TTL` value. DataStax
 Kafka Connector appends `AND TTL <configured-ttl-value>` to the
 INSERT statement for those
 rows.
Default: `-1` ( disabled )

**deletesEnabled**
: When enabled, treat records that after mapping would result in only
 non-null values for primary key columns as deletes, rather than inserting/updating
 nulls for all regular columns.
> **Note:** Note: Only triggers if the setting is enabled and the
 mapping specification contains all DSE table columns.
 
Default: `true`
