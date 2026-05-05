---
title: Using a Kerberos ticket cache to authenticate connector running on a stand-alone worker
source: security/kafkaKerberosTicket.html
---

📘 [Documentation](../README.md) > [Security](README.md) > Kerberos Ticket

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Using a Kerberos ticket cache to authenticate connector running on a stand-alone
 worker
 
Authenticate the DataStax Connector session using a Kerberos ticket with a worker
 that is running in stand-alone mode.
 
Authenticate the DataStax Apache Kafka™ Connector session using a
 Kerberos ticket with a worker that is running in stand-alone mode.
 
## Procedure

1. Put the Kerberos configuration file (krb5.conf) in
 /etc. 
> **Note:** Tip: To use an alternate location for the configuration file (other
 than the default location `/etc`), set the environment
 variable to point to the configuration file. See [Using an alternate location for the Kerberos files](kerberos-config.md).
2. Add accounts to Kerberos and the cluster: 
1. Add a service principal for the host where the connector is running in
 standalone mode. See [Add principal](http://web.mit.edu/kerberos/krb5-current/doc/admin/database.html?highlight=add%20principal#adding-modifying-and-deleting-principals).
2. Add a login-role. See [Adding roles for Kerberos
 principals](/en/dse/6.7/dse-admin/datastax_enterprise/security/Auth/secKerberosRole.md).
3. Allow write access to the tables by granting `MODIFY` permission
 on the table to the login role. See [Controlling access to keyspaces and
 tables](/en/dse/6.7/dse-admin/datastax_enterprise/security/secDataPermission.md).
3. Configure the connector as described in [Configuring the DataStax Apache Kafka Connector](../config/tasks.md)
 using [Kerberos authentication](../config/kerberos.md) parameters.
4. Get a Kerberos ticket by running `kinit` with the DataStax Apache Kafka
 Connector principal: 
```bash
kinit datastaxconnector/kafka@EXAMPLE.COM
```
5. Start the DataStax Apache Kafka Connector. See [Operations](../operations/README.md).
