---
title: Enabling Java Management Extension remote connections
source: monitoring/kafkaEnableJmx.html
---

📘 [Documentation](../README.md) > [Monitoring](README.md) > JMX Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Enabling Java Management Extension remote connections
 
Allow remote JMX connections to monitor DataStax Apache Kafka Connector
 activity.
 
Allow remote JMX (Java Management Extension) connections to monitor DataStax Apache
 Kafka™ Connector activity. 
 To enable remote JMX connections:
- **Production environment** - 
1. Enable security and authentication for remote JMX connections. See the
 [Java Documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html).
2. add the following to the Kafka Connect worker startup
 script:
```
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.ssl=true -Djava.rmi.server.hostname=worker_ip -Dcom.sun.management.jmxremote.port=custom_port"
```
- **Secured development and test environment** - Add the following to the Kafka
 Connect worker startup script (connect-standalone.sh
 /connect-distributed.sh):
```
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=worker_ip -Dcom.sun.management.jmxremote.port=custom_port"
```

 where 
- worker_ip - Hostname or IP address of the worker
 running the DataStax Connector.
- custom_port - JMX port of the worker running the
 DataStax Connector.
