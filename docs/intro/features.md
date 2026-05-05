📘 [Documentation](../README.md) > [Introduction](README.md) > Features

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Features
 
Key features in DataStax Apache Kafka Connector.
 
DataStax Apache Kafka™ Connector is the bridge that allows data to move from
 Apache Kafka to Apache Cassandra® or DataStax [supported](compatibility.md) database tables in event-driven architectures. Known in the Kafka Connect
 framework as a sink, the key features of this connector are its performance, flexibility,
 security, and visibility.
 
## Flexibility
 
The design of this sink considers the varying data structures that are found in Apache
 Kafka, and the selective mapping functionality in the DataStax Apache Kafka Connector allows
 the user to specify the Kafka fields that should be written to the database columns. This
 allows for a single connector instance to read from multiple Apache Kafka topics and write
 to many database tables, thereby removing the burden of managing several connector
 instances. Whether the Apache Kafka data is in Avro, JSON, or string format, the DataStax
 Apache Kafka Connector extends advanced parsing to account for the wide range of data
 inputs.
 
## Security
 
One of the core value propositions of DataStax databases is enterprise-grade security. With
 built-in SSL, LDAP/Active Directory, and Kerberos integration, DataStax provides the tools
 needed to achieve strict compliance regulations for the connection from client to server.
 These security features are also included in the DataStax Apache Kafka Connector, ensuring
 that the connection between the connector and the data store is secure. 
 
## Visibility
 
In complex distributed environments, environments are bound to hit points of failure.
 DataStax products account for these error scenarios and all of the intelligence of the
 DataStax Drivers is applied in the DataStax Apache Kafka Connector. Additionally, there are
 metrics included that give the operator visibility into the failure rate and latency
 indicators as the messages pass from Kafka to DataStax databases. 
 
## Highlights of the key features
 
| Feature | Summary |
| --- | --- |
| Consume Kafka Primitive data format | DataStax Apache Kafka Connector accepts Kafka record data that is in primitive type form. |
| Consume Kafka JSON data format | DataStax Apache Kafka Connector accepts Kafka record data that is valid JSON form. |
| Consume Kafka Avro data format | DataStax Apache Kafka Connector accepts Kafka record data that is valid Avro form. |
| Pluggable Connect converters | DataStax Apache Kafka Connector works with StringConverter, JsonConverter, AvroConverter, ByteArrayConverter, and Numeric Converters, as well as custom data converters. Note that the producer of the data must use the same Converter as the connector. |
| Provides JMX Metrics | DataStax Apache Kafka Connector exposes JMX metrics for record/failure count, and latency recordings. |
| Runs within Connect Worker | DataStax Apache Kafka Connector is deployed in the Kafka Connect framework. |
| At Least Once Guarantee | DataStax Apache Kafka Connector stores the offset in Kafka and will pick up where it left off if restarted. This minimizes the additional work but there are situations where writes to Cassandra or DSE will be retried if many records are in a single failed batch. DataStax Apache Kafka Connector ensures that no records are missed. |
| Runs within Connect Worker | DataStax Apache Kafka Connector is deployed in the Kafka Connect framework. |
| Standalone Mode Support | DataStax Apache Kafka Connector is deployed in Kafka Connect framework and works in standalone mode (meant for dev/test). |
| Distributed Mode / HA Support | DataStax Apache Kafka Connector is deployed in Kafka Connect framework and works in distributed mode (meant for production). |
| Flexible Kafka topic to the database's table mapping | DataStax Apache Kafka Connector extends flexible mapping functionality to control the specific fields that are pulled from Kafka and written to the [supported](compatibility.md) database types. |
| Single Kafka topic to multiple database tables | DataStax Apache Kafka Connector enables common denormalization pattern by allowing a single topic to be written to many database tables. The multiple databases must be of the same [type](compatibility.md). |
| Connector throttling and parallelism | DataStax Apache Kafka Connector has built-in throttling to limit the max concurrent requests that can be sent by a single connector instance. Parallelism is delivered through the integration with the Kafka Connect distributed framework and asynchronous connector internals. |
| Flexible date/time/timestamp formats | DataStax Apache Kafka Connector accounts for the typical case where separate teams write to the same Kafka deployment and may use varying formats for date/time fields. |
| Configurable Consistency Level | DataStax Apache Kafka Connector allows configuring the consistency levels on a per topic-table basis. |
| Row-level Time-to-Live (TTL) | DataStax Apache Kafka Connector allows configuring row-level TTL on a per topic-table basis. |
| Deletes | DataStax Apache Kafka Connector allows configuring deletes on a per topic-table basis. |
| Handling of nulls | DataStax Apache Kafka Connector allows configuring null handling on a per topic-table basis. |
| Error handling | DataStax Apache Kafka Connector has built-in error handling for various failure scenarios. These scenarios include bad mappings and database write issues. |
| Offset management | DataStax Apache Kafka Connector leverages the Kafka Connect Framework to manage offsets by storing the offset in Kafka. |
| Connector to database SSL | DataStax Apache Kafka Connector allows configuring connection to the database with SSL. |
| Connector to database username/password | DataStax Apache Kafka Connector allows configuring connection to the database with username/password. |
| Connector to the database LDAP/Active Directory | DataStax Apache Kafka Connector allows configuring the connection to the database with LDAP/Active Directory. |
| Connector to DSE Kerberos | DataStax Apache Kafka Connector allows configuring the connection to the database with Kerberos. |
| Configurable write timeout | DataStax Apache Kafka Connector allows configuring a write timeout to the database. |
| Connector to database compression | DataStax Apache Kafka Connector allows configuring connection to the database with compression strategies. |
