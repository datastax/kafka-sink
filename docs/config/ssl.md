📘 [Documentation](../README.md) > [Configuration](README.md) > SSL Configuration

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# SSL encrypted connection
 
When the cluster has client encryption enabled
 configure the SSL keys and certificates for the DataStax Apache Kafka™
 Connector.
 When the cluster has client encryption enabled, configure the SSL keys and
 certificates for the DataStax Apache Kafka™ Connector.
> **Note:** Tip: SSL
 encryption settings are configured in the Client-to-node encryption options. See Configuring SSL.
 
> **Note:** Note: You cannot use this option if specifying the [cloud.secureConnectBundle](../config/connector.md#kafkaConnector__secure_ConnectBundle) option for
 connecting to a DataStax Astra database.
 
## Parameters
 
```
ssl.provider=None
ssl.cipherSuites=
ssl.hostnameValidation=true
ssl.keystore.password=

# Path to the keystore file.
#ssl.keystore.path=

# Truststore password.
#ssl.truststore.password=

# Path to the truststore file.
#ssl.truststore.path=

# Path to the SSL certificate file, when using OpenSSL.
#ssl.openssl.keyCertChain=

# Path to the private key file, when using OpenSSL.
#ssl.openssl.privateKey=
```
 
**ssl.provider**
: SSL provider to use, if any. Valid choices: 
- None
- JDK
- OpenSSL

Defaults to None

**ssl.hostnameValidation**
: Whether to validate node hostnames when using SSL.
 
Defaults to true

**ssl.cipherSuites**
: The cipher suites to enable. Defaults to none, resulting in a "minimal quality of
 service" according to JDK documentation.

**ssl.keystore.password**
: Keystore password.

**ssl.keystore.path**
: Path to the keystore file.

**ssl.openssl.keyCertChain**
: Path to the SSL certificate file, when using OpenSSL.

**ssl.openssl.privateKey**
: Path to the private key file, when using OpenSSL.

**ssl.truststore.password**
: Truststore password.

**ssl.truststore.path**
: Path to the truststore file.
