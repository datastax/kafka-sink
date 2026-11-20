📘 [Documentation](../README.md) > [Data Mapping](README.md) > Selective Updates

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Selectively update maps and UDTs based on Kafka fields
 
Selectively update maps and UDTs based on Kafka fields.
 
In the Kafka topic mapping, you can selectively update maps and User Defined Types (UDTs)
 based on whether Kafka fields have present values. That is, any value other than
 `null`.
 
The goal is to minimize tombstones in the database. If Maps or UDTs are in the database table
 that the Kafka Connector is configured to write to, in order to minimize tombstones, you can
 use the CQL command `UPDATE` to only update the fields present in the Kafka
 record if the `nullToUnset` parameter is set to `true`. 
 To do so, you can leverage the query parameter with the `udtColNotFrozen`
 keyword. The UDT field definition must be not frozen. For example, consider the following DDL
 statement:
 
```cql
CREATE TYPE IF NOT EXISTS myudt (udtmem1 int, udtmem2 text);
```

 In order for `CREATE TYPE` in this example to work, the `myudt`
 field must have been defined
 with:
```
udtColNotFrozen myudt
```
Once you will have
 the UDT defined, you can provide a CQL query parameter. Assume you have the following mapping:
 
```
bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2
```
You
 would use an `UPDATE` command with the `udtColNotFrozen`
 keyword. For example:
 
```cql
UPDATE ks.table set udtColNotFrozen.udtmem1=:udtcol1, udtColNotFrozen.udtmem2=:udtcol2 where bigintCol=:bigintcol
```
When
 the new record arrives, DataStax Apache Kafka™ Connector sets only the not
 null fields; it will not override fields of UDT that have a null value (or are not present) in
 the Kafka record, assuming `nullToUnset` is set to `true`. The
 behavior of the Update versus Override is defined via the `nullToUnset`
 parameter. See its description in [Kafka topic-to-table
 settings](config/table.md). For related information about Kafka maps and UDTs, see [Mapping messages to table that has a User Defined Type](struct.md).
