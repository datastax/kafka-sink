📘 [Documentation](../README.md) > [Monitoring](README.md) > Failed Records

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Failed Kafka topic record metrics
 
Running total or moving averages of Kafka topic records that could not be processed by
 a DataStax Apache Kafka Connector instance.
 
You can use DataStax Apache Kafka™ Connector metrics to return a running
 total, or moving averages per a specified rate, of Kafka topic records that the connector
 instance failed to process because the mapping could not be interpreted. 
 DataStax Apach Kafka Connector provides two Mbeans for failed record counts: 
- ```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount'
```
- ```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic'
```
The `failedRecordCount` Mbean is incremented when Kafka Connector is able
 to detect that there was a failure for a specific topic, keyspace, table; which is the
 majority case. The `failedRecordsWithUnknownTopic` Mbean is incremented when
 Kafka Connector was not able to detect the Kafka topic that was associated with a failed
 record; this is an edge case. 
## Failed Record Metric Attributes
 
**Count**
: Running total of the number of failed records encountered by the connector
 instance:
```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount' 
 attribute='Count'
```
: Running total of the number of failed records encountered by the connector where the
 Kafka topic was
 unknown:
```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic' 
 attribute='Count'
```

**OneMinuteRate**
: Moving average of records that failed in the last
 minute:
```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount' 
 attribute='OneMinuteRate'
```
: Moving average of records that failed in the last minute where the Kafka topic was
 unknown:
```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic' 
 attribute='OneMinuteRate'
```

**FiveMinuteRate**
: Moving average of records that failed in the last five
 minutes.
```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount' 
 attribute='FiveMinuteRate'
```
: Moving average of records that failed in the last five minutes where the Kafka topic
 was
 unknown:
```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic'
 attribute='FiveMinuteRate'
```

**FifteenMinuteRate**
: Moving average of records that failed in the last 15
 minutes:
```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount' 
 attribute='FifteenMinuteRate'
```
: Moving average of records that failed in the last 15 minutes where the Kafka topic was
 unknown:
```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic' 
 attribute='FifteenMinuteRate'
```

**MeanRate**
: The mean average number of records that failed since this Kafka Connector instance was
 created:
```
objectName='com.datastax.kafkaconnector:connector=*,name=topic.keyspace.table.failedRecordCount' 
 attribute='MeanRate'
```
: Mean average of records that failed since this Kafka Connector instance was created,
 where the Kafka topic was
 unknown:
```
objectName='com.datastax.kafkaconnector:connector=*,name=failedRecordsWithUnknownTopic' 
 attribute='FifteenMinuteRate'
```

> [!TIP]
> See `WARN` level messages that include specific information for each record in the Kafka Connect worker log.
