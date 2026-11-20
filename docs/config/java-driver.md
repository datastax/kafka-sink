📘 [Documentation](../README.md) > [Configuration](README.md) > Java Driver Settings

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Pass Kafka Connector settings directly to the DataStax Java driver
 
Pass Kafka Connector settings to DataStax Java driver.
 In your DataStax Apache Kafka™ Connector configuration file, you can directly
 pass settings to the DataStax Java driver by using the `datastax-java-driver`
 prefix. For example:
 
```
datastax-java-driver.basic.request.consistency=ALL
```
 
## Mapping Kafka Connector settings to Java driver properties
 The following table identifies functionally equivalent DataStax Apache Kafka Connector and
 DataStax Java driver settings. 
> **Note:** Note: If you define both in your configuration, the Kafka
 Connector setting take precedence over the
 `datastax-java-driver.property-name`. If you do not
 provide either in your configuration, Kafka Connector defaults are in effect.
 
For information about the Java properties, refer to the DataStax Java driver documentation. For information
 about the Kafka Connector settings, refer to [DataStax connection](connection.md).
 
| DataStax Apache Kafka Connector setting | Using datastax-java-driver prefix |
| --- | --- |
| `contactPoints` | `datastax-java-driver.basic.contact-points` |
| `loadBalancing.localDc` | `datastax-java-driver.basic.load-balancing-policy.local-datacenter` |
| `cloud.secureConnectBundle` | `datastax-java-driver.basic.cloud.secure-connect-bundle` |
| `queryExecutionTimeout` | `datastax-java-driver.basic.request.timeout` |
| `connectionPoolLocalSize` | `datastax-java-driver.advanced.connection.pool.local.size` |
| `compression` | `datastax-java-driver.advanced.protocol.compression` |
| `metricsHighestLatency` | `datastax-java-driver.advanced.metrics.session.cql-requests.highest-latency` |
 
There is a difference between the Kafka Connector's `contactPoints` setting
 and the Java driver's `datastax-java-driver.basic.contact-points`. For
 `contactPoints`, the value of the `port` is appended to every
 host provided by this setting. For `datastax-java-driver.basic.contact-points`,
 you must provide the fully qualified contact points (`host:port`). 
 By passing in the Java driver's setting, this option gives you more configuration flexibility
 because you can specify a different port for each host. Example:
```
datastax-java-driver.basic.contact-points = 127.0.0.1:9042, 127.0.0.2:90
```
 
## Conversion of Java driver properties of type List to TypeSafe Config
 The following properties that are of type `List`, which you could pass into
 the driver from your Kafka Connector configuration via the
 `datastax-java-driver` prefix, are converted by the DataStax Java driver to
 the `TypeSafe Config` format. 
- `datastax-java-driver.advanced.ssl-engine-factory.cipher-suites`
- `datastax-java-driver.advanced.metrics.node.enabled`
- `datastax-java-driver.advanced.metadata.schema.refreshed-keyspaces`
- `datastax-java-driver.advanced.metrics.session.enabled`
- `datastax-java-driver.basic.contact-points`
The conversion by the Java driver will split the comma-separated values provided in
 those setting to create indexed properties. For example, if you passed in
 `datastax-java-driver.advanced.metrics.session.enabled=a,b`, the entry is
 converted to:
 
```
datastax-java-driver.advanced.metrics.session.enabled.0=a
datastax-java-driver.advanced.metrics.session.enabled.1=b
```
For
 more information, refer to the Java driver reference configuration topic.
