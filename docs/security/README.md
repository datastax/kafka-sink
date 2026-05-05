📘 [Documentation](../README.md) > Security

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Security
 
Configure security between the DataStax Apache Kafka™ Connector and the cluster.

## Topics

### Authentication Methods

- **[Using internal or LDAP authentication](ldap-auth.md)** - Authenticate the DataStax Apache Kafka Connector session using internal or LDAP authentication
- **[Using the DataStax Apache Kafka Connector with Kerberos](kerberos-auth.md)** - Authenticate the DataStax Apache Kafka Connector session using Kerberos

### Kerberos Configuration

- **[Using an alternate location for the Kerberos files](kerberos-config.md)** - Add custom paths to Kafka Connect and Kerberos environment variables when using a location other than /etc for the krb5.conf file
- **[Using a Kerberos ticket cache to authenticate connector running on a stand-alone worker](kerberos-ticket.md)** - Authenticate the DataStax Connector session using a Kerberos ticket with a worker that is running in stand-alone mode

## Related Documentation

- [Configuration Reference](../config/README.md) - See authentication and SSL configuration options
- [Installation Guide](../install/README.md)
- [Troubleshooting](../troubleshooting/README.md)
