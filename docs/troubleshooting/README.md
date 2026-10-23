📘 [Documentation](../README.md) > Troubleshooting

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md) | [Cycling comments example](../cycling-comments-example.md)

---

# Troubleshooting the DataStax Apache Kafka Connector
 
Find answers to common issues and errors.

DataStax Apache Kafka Connector writes error messages to the Kafka Connect Worker log. The default logger is `log4j`, which is configured in the connect-log4j.properties file distributed with Apache Kafka and Confluent. If a file is not designated, the logger writes to `stdout`.

> [!NOTE]
> When the Kafka Connect worker first starts, many informational messages are written to the log. Most messages are related to system configuration. These messages are harmless and contain startup and configuration information.

## Contents

*No specific troubleshooting guides are currently available. Check the logs for error messages and refer to the related documentation below.*

## Related Documentation

- [Monitoring](../monitoring/README.md) - Monitor connector activity to identify issues
- [Configuration Reference](../config/README.md) - Verify your configuration settings
- [Security](../security/README.md) - Troubleshoot authentication and security issues
- [Operations Guide](../operations/README.md) - Operational best practices
