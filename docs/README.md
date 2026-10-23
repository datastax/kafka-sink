# DataStax Apache Kafka Connector Documentation

---

**Quick Links:** [Home](#) | [Install](install/README.md) | [Config](config/README.md) | [Operations](operations/README.md) | [Monitoring](monitoring/README.md) | [FAQ](faq.md) | [Troubleshooting](troubleshooting/README.md) | [Cycling comments example](cycling-comments-example.md)

---

This documentation covers the DataStax Apache Kafka Connector, which enables you to stream data from Apache Kafka topics to DataStax Enterprise (DSE) and Apache Cassandra databases.

## Quick Links

- [Introduction](intro/README.md) - Overview, architecture, and features
- [Installation](install/README.md) - Installation instructions
- [Configuration](config/README.md) - Configuration reference
- [Cycling comments example](cycling-comments-example.md) - Step-by-step example of mapping Kafka topic records to `cycling.comments`
- [Data Mapping](mapping/README.md) - How to map Kafka topics to database tables
- [Operations](operations/README.md) - Operational procedures
- [Security](security/README.md) - Security configuration
- [Monitoring](monitoring/README.md) - Metrics and monitoring
- [Troubleshooting](troubleshooting/README.md) - Common issues and solutions
- [FAQ](faq.md) - Frequently asked questions (compatibility, installation, configuration, etc.)
- [Release Notes](release-notes/README.md) - Version history and changes

## Getting Started

1. **[Install the connector](install/README.md)** - Download and install the connector
2. **[Configure connection](config/connection.md)** - Set up connection to your database
3. **[Map topics to tables](mapping/README.md)** - Configure data mapping
4. **[Monitor performance](monitoring/README.md)** - Set up monitoring and metrics

## Key Features

- **High Performance**: Optimized for high-throughput data ingestion
- **Flexible Mapping**: Support for JSON, Avro, and other data formats
- **Multiple Tables**: Route data from topics to multiple tables
- **Security**: Support for SSL/TLS, Kerberos, and LDAP authentication
- **Monitoring**: Comprehensive JMX metrics for monitoring

## Architecture

The DataStax Apache Kafka Connector is a Kafka Connect sink connector that reads data from Kafka topics and writes it to DataStax Enterprise or Apache Cassandra tables.

![Architecture Overview](images/kafkaOverview.png)

For more details, see the [Architecture documentation](intro/architecture.md).

## Support

For issues, questions, or contributions, please refer to the main repository.

## License

See [LICENSE.txt](../LICENSE.txt) for license information.
