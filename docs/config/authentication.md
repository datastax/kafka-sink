---
title: Using the DataStax Connector with DataStax Enterprise authentication
source: configuration_reference/kafkaAuth.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > Authentication

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Using the DataStax Connector with DataStax Enterprise authentication
 
When authentication is enabled, provide credentials for internal
 authentication, LDAP, or location of the Kerberos keytab file. 
 
cassandra.yaml
The location of
 the cassandra.yaml file depends on the type of
 installation:
| Package installations | /etc/dse/cassandra/cassandra.yaml |
| Tarball installations | installation_location/resources/cassandra/conf/cassandra.yaml |

dse.yaml
The location of the
 dse.yaml file depends on the type of
 installation:
| Package installations | /etc/dse/dse.yaml |
| Tarball installations | installation_location/resources/dse/conf/dse.yaml |
 When authentication is enabled, provide credentials for
 internal authentication, LDAP, or location of the Kerberos keytab file. 
> **Note:** Tip: To
 verify that authentication is configured, check the following parameters:
- authenticator of the
 `cassandra.yaml`
- authentication_options of the
 `dse.yaml`
