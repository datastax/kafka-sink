---
title: Using internal or LDAP authentication
source: security/kafkaInternalLdapAuth.html
---

📘 [Documentation](../README.md) > [Security](README.md) > LDAP Authentication

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Using internal or LDAP authentication
 
Authenticate the DataStax Apache Kafka Connector session using internal or LDAP
 authentication. 
 Authenticate the DataStax Apache Kafka™ Connector session using
 internal or LDAP authentication. When the cluster has internal or LDAP authentication enabled,
 configure the user name and password settings for DataStax Apache Kafka™
 Connector. This task also requires a login role for the DataStax Apache Kafka™
 Connector with access to the mapped tables.
dse.yaml
The location of the
 dse.yaml file depends on the type of
 installation:
| Package installations | /etc/dse/dse.yaml |
| Tarball installations | installation_location/resources/dse/conf/dse.yaml |

cassandra.yaml
The location of
 the cassandra.yaml file depends on the type of
 installation:
| Package installations | /etc/dse/cassandra/cassandra.yaml |
| Tarball installations | installation_location/resources/cassandra/conf/cassandra.yaml |
 
## Procedure

1. Verify whether your cluster has internal or LDAP authentication enabled. Refer to the
 authentication options in the
 `dse.yaml` file. Authentication options for the DSE
 Authenticator allow you to use multiple schemes for authentication simultanelously in a
 DataStax Enterprise cluster.
2. Additional authenticator configuration is required in
 `dse.yaml` when the authorization option in
 `cassandra.yaml` is set to
 `com.datastax.bdp.cassandra.auth.DseAuthorizer`. When authorization is
 enabled, the DataStax Apache Kafka Connector login role must have a minimum of
 `modify` privileges on tables receiving data from the DataStax Apache
 Kafka Connector. For details, refer to the authorization options.
3. In `dse.yaml`, there are options to configure LDAP
 security when the authenticator option in `cassandra.yaml`
 is set to `com.datastax.bdp.cassandra.auth.DseAuthenticator`. When not set,
 LDAP authentication is not used. The default in dse.yaml is that this setting is commented
 out (false). For details, refer to Defining an LDAP scheme.
4. Internal and LDAP schemes can also be used for role management. Refer to the role management options.
5. Set the relevant auth parameters in `dse.yaml` for
 `auth.provider`, `auth.username` (the login role name or
 LDAP username), and `auth.password`. See [parameters](ldap-auth.md).
