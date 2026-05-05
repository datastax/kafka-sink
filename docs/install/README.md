📘 [Documentation](../README.md) > Installation

---

**Quick Links:** [Home](../README.md) | [Install](#) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Installing DataStax Apache Kafka Connector 1.7.4
 
Install on Linux-based platform using a binary tarball.
 
Install DataStax Apache Kafka™ Connector 1.7.4 from the DataStax distribution tar file using an account that
 has write access to the Kafka configuration directory.
 
The supported operating systems are Linux and macOS.
 
**[cassandra-sink-distributed.json.sample](../../dist/conf/cassandra-sink-distributed.json.sample)**
 
- The
 [cassandra-sink-distributed.json.sample](../../dist/conf/cassandra-sink-distributed.json.sample)
 file is located in the conf
 directory of the DataStax Apache Kafka Connector
 distribution package.
 
**[cassandra-sink-standalone.properties.sample](../../dist/conf/cassandra-sink-standalone.properties.sample)**
 
- The
 [cassandra-sink-standalone.properties.sample](../../dist/conf/cassandra-sink-standalone.properties.sample)
 file is located in the conf
 directory of the DataStax Apache Kafka Connector
 distribution package.
 
## Supported Apache Kafka versions
 Install the DataStax Apache Kafka® Connector on any of following versions:
- Apache Kafka 3.0 and higher (tested with 3.7.2)
- Confluent Platform 7.0 and higher (tested with 7.7.2)
- Earlier versions: Kafka 0.10.2+ and Confluent 3.2+ may work but are not actively tested
 
## Prerequisites
 
## Procedure
 
> **Important:** Important: [Apache-2.0 license agreement](https://www.apache.org/licenses/LICENSE-2.0). By downloading
 this DataStax product, you agree to the terms of the open-source Apache-2.0 license agreement.
 
Perform the following steps on a Kafka Connect node:
 
1. Download the tar file from the [DataStax downloads site](https://downloads.datastax.com/#akc). If you agree, enable the
 Terms checkbox and click the download icon.
2. Extract the files:
```bash
tar zxf kafka-connect-cassandra-sink-1.7.4.tar.gz
```
 The following files are unpacked into a directory such as
 `kafka-connect-cassandra-sink-1.7.4`.
```
LICENSE.txt
README.md
THIRD-PARTY.txt
conf/cassandra-sink-distributed.json.sample
conf/cassandra-sink-standalone.properties.sample
kafka-connect-cassandra-sink-1.7.4.jar
```
3. Configured the DataStax connector JAR using one of the following methods:
- Move the DataStax connector JAR to the Kafka plugins
 directory:
```bash
mv installation_location/kafka-connect-cassandra-sink-1.7.4.jar kafka_plugins_dir
```
- Configure the path to the JAR:Apache Kafka 0.11.x and later - Specify the JAR location in the
 `plugin.path` parameter in the
 `connect-standalone.properties` or
 `connect-distributed.properties` file that is passed to the worker
 start-up scripts.
 Example:
```
plugin.path=install_location/kafka-connect-cassandra-sink-1.7.4.jar
```

> **Note:** Note: Confluent
 3.3 and later are supported.
4. Copy the sample configuration file from `kafka-connect-cassandra-sink-1.7.4/conf/` to the Kafka configuration directory,
 which is typically the `config` or etc directory. DataStax provides the following sample files in the
 conf directory of the connector distribution package:
- **[cassandra-sink-standalone.properties.sample](../../dist/conf/cassandra-sink-standalone.properties.sample)** for
 standalone mode. It is a Java properties file that contains all settings with
 descriptions. Settings with a default value are commented out.
- **[cassandra-sink-distributed.json.sample](../../dist/conf/cassandra-sink-distributed.json.sample)** for distributed
 mode. This file is in JSON format and contains all settings, which are enumerated and
 active. To use the default values, remove settings from the configuration file. JSON does
 not support comments.
5. Rename the sample file to cassandra-sink.properties or
 cassandra-sink.json. 
> **Note:** Attention: If you will use DataStax Apache Kafka™ Connector to
 stream records with a DataStax Astra cloud database, refer to the Astra documentation
 for information about specifying the [secure connect
 bundle](/en/astra/aws/doc/dscloud/astra/dscloudStartKafka.md) in the distributed cassandra-sink.json file. The
 secure connect bundle ZIP (downloaded via the Astra console) contains the security
 certificates and credentials for your Astra database.
6. Update the settings as necessary. See [Configuring the DataStax Apache Kafka Connector](../config/tasks.md).
7. Ensure that the user running Kafka has permission to access the configuration and JAR
 files.
8. Next, refer to the DataStax Apache Kafka Connector [introduction](../monitoring/README.md).
