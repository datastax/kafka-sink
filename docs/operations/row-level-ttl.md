📘 [Documentation](../README.md) > [Operations](README.md) > Row-Level TTL

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Setting row-level TTL values from Kafka fields
 
Set row-level TTL from Kafka fields.
 In the Kafka topic mapping, you can optionally specify which column should be used as the ttl
 (time-to-live) of the record being inserted into the database table. The mapping setting
 provides a special property, `__ttl`. The format
 is:
```
topic.my_topic.my_ks.my_table.mapping=col1=key.f1, col2=value.f1, __ttl=value.f2
```
 When you specify a `ttl` value to be used while inserting the record, by
 mapping __ttl or .ttl static configuration, you can further specify a
 `timeUnit`. The default is `SECONDS`. If you want to specify a
 `ttl` value in a unit such as `HOURS`, set
 `timeUnit` to `HOURS`. DataStax Apache Kafka Connector
 automatically transforms the value to `SECONDS`, which is the DSE
 `ttl` unit. The transformation also applies for static
 .ttl field configuration:
 
```
topic.my_topic.my_ks.my_table.ttlTimeUnit=SECONDS
```
 
If the `ttl` is provided via mapping, such as
 `topic.my_topic.my_ks.my_table.mapping=__ttl=value.f2`, and by the
 .ttl static configuration
 (`topic.my_topic.my_ks.my_table.ttl=86400`) the `ttl` value
 from the mapping takes precedence and is used. If you set `ttl` in both
 resources, DataStax Apache Kafka Connector writes a warning message in the log file.
 
When you configure
 `topic.<topic-name>.<keyspace-name>.<table-name>.ttl`, all rows for
 that topic table will have this same `TTL` value. DataStax Kafka Connector
 appends `AND TTL <configured-ttl-value>` to the INSERT
 statement for those rows.
 
> **Note:** Note: When doing dynamic configuration, if the `ttl` field value is negative, an
 error is thrown during the insert of the record into a database table. However, because the
 Kafka connector is a streaming system, DataStax Apache Kafka Connector resorts to inserting
 the record without the `ttl` value.
