---
title: Configure logging for Kafka Connector
source: kafkaConfigureLogging.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > Logging

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Configure logging for Kafka Connector
 
Configure logging for DataStax Apache Kafka Connector.
 There are two ways to configure logging for DataStax Apache Kafka™ Connector.
1. Add an entry for `com.datastax.oss.kafka.sink` loggers in
 etc/kafka/connect-log4j.properties. This file is used automatically
 by Kafka Connector at startup. For example, if you want to set the log level for this
 connector to `DEBUG`, you can add the following entry in the properties
 file: 
```
com.datastax.oss.kafka.sink=DEBUG
```

 You can configure the setting to operate at lower granularity level. Example:
 
```
com.datastax.oss.kafka.sink.CassandraSinkTask=ERROR 
```
2. Or dynamically configure the loggers of your Kafka Connector. Once your Kafka Connect
 API is running, retrieve all dynamic logger configurations using a `curl`
 command. (These examples specify `localhost`; provide the address of the
 running connect worker.)
 
```bash
curl -s http://localhost:8083/admin/loggers/ | jq
```

 To dynamically set an `ERROR` log level for
 `com.datastax.oss.kafka.sink`, submit a `PUT` request via
 `curl`:
 
```bash
curl -s -X PUT -H "Content-Type:application/json" \
 http://localhost:8083/admin/loggers/com.datastax.oss.kafka.sink \
 -d '{"level": "ERROR"}' \
 | jq '.'
```
 When you retrieve loggers again, as in the following example, upper and lower level
 granularity loggers are set:
 
```bash
curl -s http://localhost:8083/admin/loggers/com.datastax.oss.kafka.sink | jq
```

```
 "com.datastax.oss.kafka.sink": {
 "level": "ERROR"
 },
 "com.datastax.oss.kafka.sink.CassandraSinkTask": {
 "level": "ERROR"
 },
```
 Again, you can set log level dynamically at a lower granularity level, such as
 `WARN`. Example:
 
```bash
curl -s -X PUT -H "Content-Type:application/json" \
 http://localhost:8083/admin/loggers/com.datastax.oss.kafka.sink.CassandraSinkTask \
 -d '{"level": "WARN"}' \
 | jq '.'
```
