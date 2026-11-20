📘 [Documentation](../README.md) > Operations

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md) | [Cycling comments example](../cycling-comments-example.md)

---

# Operations

This section contains documentation about operating and managing the DataStax Apache Kafka Connector, including advanced mapping features, query customization, and data management techniques.

## Topics

### Advanced Mapping Features

- **[Provide CQL queries in mappings](cql-queries.md)** - Optionally provide a CQL query that runs when each new record arrives in the Kafka topic mapping
- **[The now() function in mappings](now-function.md)** - Use the `now()` function in your topic-to-table mappings to generate timestamps
- **[Setting row-level TTL values from Kafka fields](row-level-ttl.md)** - Configure Time-to-Live (TTL) values at the row level based on Kafka message fields

### Data Management

- **[Determining topic data structure](displaying-topics.md)** - Understand the structure of data in your Kafka topics to properly configure mappings

## Related Documentation

- [Configuration Reference](../config/README.md)
- [Mapping Topics to Tables](../mapping/README.md)
- [Monitoring](../monitoring/README.md)
