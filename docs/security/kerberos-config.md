📘 [Documentation](../README.md) > [Security](README.md) > Kerberos Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Using an alternate location for the Kerberos files
 
When using a location other than /etc for the
 `krb5.conf` file, add the custom path to Kafka Connect and Kerberos
 environment variables.
 When using a location other than /etc for the krb5.conf file, add the custom path to Kafka Connect
 and Kerberos environment variables. If the Kerberos configuration file is not available
 on the system, get it from the Kerberos system administrator. 
> **Note:** Tip: See [Default paths](http://web.mit.edu/kerberos/krb5-current/doc/mitK5defaults.html#paths).
 
## Procedure
Kerberos clients
- Configure the path to file for Kerberos clients, such as
 `kinit`, `klist`, and
 `kdestroy`. Set the path to the file in the `KRB5_CONFIG` environment
 variable:
```bash
export KRB5_CONFIG="path_to_file"
```
Kafka Connect framework
- Configure the path to the file for the DataStax Apache Kafka Connector. Add the system property, `java.security.krb5.conf`, to the
 `KAFKA_OPTS` environment
 variable:
```bash
export KAFKA_OPTS=$KAFKA_OPTS -Djava.security.krb5.conf="path_to_file"
```
 
> **Note:** Note: Both `connect-standalone` and
 `connect-distributed` support specifying extra JVM
 options through the `KAFKA_OPTS` environment variable.
