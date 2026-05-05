📘 [Documentation](../README.md) > [Monitoring](README.md) > Write Metrics

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# DataStax Apache Kafka Connector - Batch size metrics
 
Write statistics for each mapping of a Kafka topic to a database
 table.
 
The DataStax Apache Kafka™ Connector records metrics for each Kafka topic
 that is synchronized with a [supported
 database](../kafkaIntro.md#kafkaIntro__kafkaIntroduction) table. The metric corresponds to a topic mapping. See [Mapping kafka topics to database tables](README.md).
 Two related metrics are provided:
 
- `batchSize`
- `batchSizeInBytes`
 The Mbean paths are:
- ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize'
```
- ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes'
```
where the variables above correspond to settings
 in the connector configuration file:
- connector_name - DataStax Apache Kafka® Connector [name](../config/connector.md#kafkaConnector__name).
- topic_name is Kafka [prefix](../config/table.md#kafkaCassandraTable__prefix).
- keyspace_name is the case-sensitive database [keyspace name](../config/table.md#kafkaCassandraTable__DseKeyspaceName).
- table_name is the case-sensitive database [table name](../config/table.md#kafkaCassandraTable__DseTableName).
 
## About the batch metrics
 
The `batchSize` metric returns the number of statements in the CQL batch used to write records to the database. The connector
 default limit is 32. See [maxNumberOfRecordsInBatch](../config/connection.md#kafkaDseConnection__maxNumberOfRecordsInBatch). 
 
The `batchSizeInBytes` metric is a histogram with the calculated size of
 every batch statement. The calculated value is saved in bytes. You can retrieve the
 distribution of values of every batch statement. The algorithm used to calculate batch size
 evaluates the data size contained in the given statement. The data size is the total number
 of bytes required to encode all the bound variables contained in the statement.
 
If the limit is consistently hit, then increasing the batch size value can result in better
 Kafka Connector throughput if the database nodes can handle the added pressure of larger CQL
 batch statements. When increasing [maxNumberOfRecordsInBatch](../config/connection.md#kafkaDseConnection__maxNumberOfRecordsInBatch), ensure that the batch size does
 not exceed the maximum allowed by batch_size_fail_threshold_in_kb. See [Troubleshooting](../troubleshooting/README.md) for details. 
 
## Attributes for batchSize and batchSizeInBytes
 The attributes are:
 
 
**Mean**
: The mean average number of write requests per batch
 statement.
```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='Mean'
```
: The mean average size in bytes of write requests per batch
 statement.
```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='Mean'
```

**Count**
: Running number total of all batch sizes.
 
```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='Count'
```
: Running total of all batch sizes in bytes.
 
```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='Count'
```

**50thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='50thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='50thPercentile'
```

**75thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='75thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='75thPercentile'
```

**95thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
table=table_name,name=batchSize' attribute='95thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='95thPercentile'
```

**98thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='98thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='98thPercentile'
```

**99thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='99thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='99thPercentile'
```

**999thPercentile**
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSize' attribute='999thPercentile'
```
: ```
objectName='com.datastax.kafkaconnector:connector=connector_name,topic=topic_name,keyspace=keyspace_name,
 table=table_name,name=batchSizeInBytes' attribute='999thPercentile'
```
