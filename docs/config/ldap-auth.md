---
title: Internal or LDAP authentication
source: configuration_reference/kafkaAuthLdap.html
---

📘 [Documentation](../README.md) > [Configuration](README.md) > LDAP Authentication

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Internal or LDAP authentication
 
When the cluster has internal or LDAP authentication
 enabled, you should configure the username and password settings for the Kafka Connector.
 
When the cluster has internal or LDAP authentication enabled, configure the user
 name and password settings for DataStax Apache Kafka™ Connector. A login
 role for the connector with access to the mapped tables is also required.
 
## Parameters
 
```
auth.provider=PLAIN
auth.username=login_role
auth.password=password
```
 
**auth.provider**
: Select the type of authentication provider configured for the DataStax cluster. 
- **None** - No authentication.
- **PLAIN** - Internal or LDAP authentication.
- **GSSAPI** - Supports SASL authentication to DSE clusters using the GSSAPI
 mechanism (Kerberos authentication)
> **Note:** Note: When using the GSSAPI, the Kafka Connect
 process requires that the Kerberos configuration file (krb5.conf) location is provided in the
 `java.security.krb5.conf` system property at startup. See
 [Using the DataStax Apache Kafka Connector with Kerberos](../security/kerberos-auth.md).

Default: `None`

**auth.username**
: DSE login role name or LDAP username.Astra database username.
> **Note:** Note: When [authorization is
 enabled](/en/dse/6.8/dse-admin/datastax_enterprise/config/configDseYaml.md#configDseYaml__authorizationOptions), the DataStax connector login role must have a minimum of
 `modify` privileges on tables receiving data from the DataStax Apache
 Kafka® Connector.

**auth.password**
: Login role or LDAP password.Astra
 database password for the specified username.
