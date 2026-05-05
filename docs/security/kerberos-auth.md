---
title: Using the DataStax Apache Kafka Connector with Kerberos
source: security/kafkaKerberosAuth.html
---

📘 [Documentation](../README.md) > [Security](README.md) > Kerberos Authentication

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Using the DataStax Apache Kafka Connector with Kerberos
 
Authenticate the DataStax Apache Kafka Connector session using Kerberos.
 When connecting to a cluster that has Kerberos enabled, set up the following: 
- Service accounts:
- **Kerberos principal** - Service account used by the connector to start a session
 with the cluster. See [Principals](https://web.mit.edu/kerberos/krb5-latest/doc/admin/database.html#principals).
- **Database log-in role** Role that matches the Kerberos principal name
 (case-sensative) with login set to `true`. See Adding roles for Kerberos principals.
 
> **Note:** Note: When authorization is also enabled, the role must have modify permissions on
 all tables mapped to a Kafka topic. See Controlling access to keyspaces and tables.
- On the **Kafka Connect** nodes:
- Install `kinit`. This utility obtains and caches Kerberos tickets
 used for authentication. See [**Installing and configuring UNIX client
 machines**](https://web.mit.edu/kerberos/krb5-latest/doc/admin/install_clients.html).
- Setup krb5.conf, which identifies the KDS (key distribution
 server), Kerberos administration server, and contains other settings. Required for
 Kerberos client tools, such as `kinit`. See [**Client machine configuration
 files**](https://web.mit.edu/kerberos/krb5-latest/doc/admin/install_clients.html#client-machine-configuration-files).
- Create a keytab file. A keytab file contains a certificate that allows the connector
 to obtain credentials without re-entering the password each time it initiates session
 with the cluster.
- Set Kerberos parameters in the DataStax Connector configuration file.
