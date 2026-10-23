📘 [Documentation](../README.md) > [Operations](README.md) > Displaying Topics

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Determining topic data structure
 
Display messages to determine the data structure of the topic messages.
 
One way to determine the format of pre-existing data in Kafka is to run the command
 line consumer and look at what is in the topic.
 
## Procedure

- To show Apache Kafka messages: 
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--from-beginning --property print.key=true --max-messages 5 \
--topic topic_name
```
- To show Confluent Kafka messages: 
- Messages other than
 Avro:
```bash
bin/kafka-console-consumer --bootstrap-server localhost:9092 \
--from-beginning --property print.key=true --max-messages 5 \
--topic topic_name
```
- Avro
 messages
```bash
bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 \
--from-beginning --property print.key=true --max-messages 5 \
--topic topic_name
```
