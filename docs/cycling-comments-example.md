📘 [Documentation](README.md) > Cycling comments example

---

**Quick Links:** [Home](README.md) | [Install](install/README.md) | [Config](config/README.md) | [Operations](operations/README.md) | [Monitoring](monitoring/README.md) | [FAQ](faq.md) | [Troubleshooting](troubleshooting/README.md)

---

# Cycling comments example

The cycling comments example shows how to map JSON records from a Kafka topic into the `cycling.comments` table.

> [!IMPORTANT]
> This example is intended for test or demonstration environments where Apache Kafka and the target database run on the same system.

### What this example demonstrates

- Creating a Kafka topic for JSON records
- Creating a `cycling.comments` table
- Configuring the connector to map topic fields to table columns
- Producing sample records to Kafka
- Verifying that records were written to the database

### Before you begin

This example assumes that Kafka Connect, Apache Kafka, and your target database are already installed and available locally.

For connector installation and initial setup, see the [Installation Guide](install/README.md).

### 1. Create the Kafka topic

Create the `CyclingComments` topic:

```bash
kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic CyclingComments
```

### 2. Create the keyspace and table

Use `cqlsh` to create the `cycling` keyspace and `comments` table:

```sql
CREATE KEYSPACE cycling WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE cycling.comments (
    id UUID,
    created_at TIMESTAMP,
    comment TEXT,
    commenter TEXT,
    record_id TIMEUUID,
    PRIMARY KEY (id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

### 3. Create the connector configuration

Create a file named `cycling-comments-sink.json` with the following contents:

```json
{
  "name": "cycling-comments-sink",
  "config": {
    "connector.class": "com.datastax.kafkaconnector.DseSinkConnector",
    "tasks.max": "1",
    "topics": "CyclingComments",
    "topic.CyclingComments.cycling.comments.mapping": "record_id=value.rid,id=value.id,commenter=value.author,comment=value.comment,created_at=value.created_at"
  }
}
```

Register the connector:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d @cycling-comments-sink.json \
  http://localhost:8083/connectors
```

Verify connector status:

```bash
curl -X GET "http://127.0.0.1:8083/connectors/cycling-comments-sink/status"
```

### 4. Produce sample data

Create a file named `data_all.json` with newline-delimited JSON records such as:

```json
{"id":"e7ae5cf3-d358-4d99-b900-85902fda9bb0","created_at":"2017-04-01 14:33:02.160Z","comment":"LATE RIDERS SHOULD NOT DELAY THE START","author":"Alex","rid":"22d496d1-cf24-11e8-a84b-2b44b2d77e7c"}
{"id":"c7fceba0-c141-4207-9494-a29f9809de6f","created_at":"2018-10-13 20:11:14.503Z","comment":"The gift certificate for winning was the best","author":"Amy","rid":"22d61d71-cf24-11e8-a84b-2b44b2d77e7c"}
{"id":"fb372533-eb95-4bb4-8685-6ef61e994caa","created_at":"2018-10-13 20:11:14.564Z","comment":"Great course","author":"Michael","rid":"22df6c42-cf24-11e8-a84b-2b44b2d77e7c"}
```

Load the data into Kafka:

```bash
kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic CyclingComments < data_all.json
```

### 5. Verify the results

Consume the topic from the beginning:

```bash
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic CyclingComments \
  --from-beginning
```

Count the records written to the database:

```bash
dsbulk count -k cycling -t comments
```

> [!NOTE]
> The topic name is `CyclingComments`, and the mapped table is `cycling.comments`.

## Additional Resources

- [Installation Guide](install/README.md) - Install and configure the connector
- [Configuration Reference](config/README.md) - Complete configuration options
- [Data Mapping Guide](mapping/README.md) - Learn how to map Kafka topics to database tables
- [FAQ](faq.md) - Frequently asked questions