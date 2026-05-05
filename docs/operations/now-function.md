📘 [Documentation](../README.md) > [Operations](README.md) > NOW Function

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# The now() function in mappings
 
You can use the now() function in mappings.
 
In the Kafka topic mapping, you can use the `now()` function. It returns
 `TIMEUUID`, which is documented in the UUID and timeuuid column.
 For
 example:
```
"topic.my_topic.my_ks.my_table.mapping": "col1=now()"
```
 In this example, you must create `col1` in a CQL table with the following
 definition: 
```
col1 timeuuid
```
You can use the
 `now()` function for mapping of multiple database columns:
 
```
"topic.my_topic.my_ks.my_table.mapping": "col1=now(), col2=now(), col3=now()"
```
In
 the example above, each occurrence of `now()` is evaluated and return a
 different `TIMEUUID`. 
## Behavior of now() with DELETE records
 When the value of a record arrives to DataStax Apache Kafka Connector instance, it could be
 a `DELETE`, assuming `deletesEnabled` is
 `true`; see [Kafka topic-to-table settings](../config/table.md). For example, consider a case
 where a user has mapping and used the `now()` function:
 
```
my_pk=value.my_pk, my_value=value.my_value, loaded_at=now()
```
If
 there is an insertion of a value that is `null`, such as:
 
```
"{\"my_pk\": 1234567, \"my_value\": null}";
```
In
 this scenario, the called `now()` function is ignored, and the record is
 deleted.
