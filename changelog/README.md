## Changelog

### 1.3.1
- [bug] KAF-189: Tinkerpop dependency in driver is required, while it should not be

### 1.3.0
- [improvement] KAF-98: Selectively update Maps and UDTs based on present Kafka fields
- [improvement] KAF-161: Set ApplicationName and ClientId for DseProgrammaticBuilder to identify kafka-connector in Insights
- [improvement] KAF-170: Update Kafka Connector to unified driver
- [improvement] KAF-108: Validate Connect Converter Headers support with our connector and upgrade to kafka 2.4
- [new feature] KAF-108: Support providing CQL query
- [new feature] KAF-127: Add support for now()
- [improvement] KAF:100: Add rates to failedRecordCount
- [new feature] KAF-99: Add new BatchSizeInBytes metric
- [improvement] KAF-163: Add test of auth configuration inferring auth.provider to DSE when both username and password are provided.	

### 1.2.1
- [improvement] KAF-165: Remove restriction preventing Kafka Connector use with open source Cassandra

### 1.2.0
- [new feature] KAF-143: Add support for connecting with cloud using secureBundle
- [new feature] KAF-142: Add ability to extract fields from headers in kafka record and use them in mapping
- [improvement] KAF-148: Switch from using dsbulk kafka_ft branch to 1.x branch
- [task] KAF-151: Evaluate impacts of DAT-449
- [improvement] KAF-80: Ability to keep contact points unresolved
- [improvement] KAF-79: Expose driver configuration options

### 1.1.1
- [improvement] KAF-132: add ignoreErrors settings to prevent infinite loop of errors
- [bug] KAF-135: dse-reference.conf is not loaded because wrong config Loader is used
- [improvement] KAF-136: Upgrade DSE and OSS driver dependencies

### 1.1.0
- [improvement] KAF-115: Configure advanced.metrics.session.cql-requests.highest-latency according to request-timeout
- [improvement] KAF-113: Support deleting rows in DSE when value is null in kafka record
- [improvement] KAF-81: Increment kafka-connect-api version to newest one (2.0.0)
- [improvement] KAF-112: Upgrade to DSE Driver 2.0.1 GA
- [improvement] KAF-72: Counters and histograms should be per topic/keyspace/table, not global
- [new feature] KAF-46: Allow mapping of a field in the Kafka record to the WRITETIME of the DSE record
- [bug] KAF-104: Support topic names that contain periods
- [new feature] KAF-107: Allow ttl to be set from a Kafka field

### 1.0.0
- [new feature] KAF-49: Add queryExecutionTimeout global setting
- [improvement] KAF-52: Refactor kafka-sink repo into a few Maven modules
- [improvement] KAF-54: Change default-properties to be more intuitive
- [bug] KAF-55 Potential deadlock during connector shutdown
- [bug] KAF-53: Jar file name is wrong
- [bug] KAF-57: CL for one topic/table may mask another
- [bug] KAF-59: LifeCycleManager.stopTask has a race condition
- [new feature] KAF-60 Make BoundStatements limit for the batches configurable
- [bug] KAF-64: BoundStatementProcessor should not block indefinitely
- [bug] KAF-70: recordCount is not accurate
- [bug] KAF-76: Connector should fail fast if contactPoints are not valid
- [bug] KAF-85: Metrics for case-sensitive table names cannot be exposed with JMX
- [new feature] KAF-95: Expose connectionsPerHost in the connector configuration

### 1.0.0-alpha3
- [bug] KAF-39: Partitions don't rebalance in distributed mode across multiple workers
- [new feature] KAF-18: Add SSL support
- [new feature] KAF-19: Add plaintext authentication
- [new feature] KAF-20: Add Kerberos authentication
- [new feature] KAF-5: Support Counter type
- [bug] KAF-41: Byte array values cause CodecNotFoundException
- [improvement] KAF-10: Disallow connecting to C* nodes
- [improvement] KAF-15: Gather metrics and report via JMX
- [new feature] KAF-21: Delete row when record has no value
- [new feature] KAF-45: Add 'compression' option to support sending compressed requests to DSE
- [improvement] KAF-43: Add ability to map a single topic to multiple tables
- [bug] KAF-47: Connector sometimes loses track of a failed record

### 1.0.0-alpha2
- [improvement] KAF-38: Improve performance by parallelizing record mapping and using partition-key-based batches when possible
- [bug] KAF-36: Kafka Sink fails when optional schema types are used in Avro schema
- [bug] KAF-37: Using a boolean field in a UDT causes CodecNotFoundException
- [bug] KAF-40: When testing against DSE 5.0, tests fail because DateRange type doesn't exist

### 1.0.0-alpha1
- [new feature] KAF-1: Support JSON Records
- [improvement] KAF-2: Replace duped dsbulk commons code with a ref to the new dsbulk-commons-1.2.0-ng
- [improvement] KAF-3: Expand Kafka Struct support to handle complex objects
- [new feature] KAF-4: Add support for remaining C* and DSE datatypes except Counter and DateRange
- [new feature] KAF-6: Support specifying TTL for inserted rows
- [new feature] KAF-25: Set timestamp on DSE row to the timestamp in the Kafka record
- [improvement] KAF-7: Support mapping raw values in records (e.g. mycol=value or mycol=key)
- [improvement] KAF-12: Fix setting names to conform to design doc
- [improvement] KAF-8: Use batch requests to load data
- [improvement] KAF-27: Produce a tarball deliverable in build process
- [improvement] KAF-26: Support configurable time-zone and locale for date/time parsing
- [new feature] KAF-11: Add nullToUnset topic setting to allow nulls to be treated as nulls
- [improvement] KAF-29: Date/Time conversion settings should be topic-scoped
- [improvement] KAF-13: Exclude unnecessary classes from uberjar
- [new feature] KAF-9: Support specifying consistency level for statements
- [bug] KAF-30: Connector throws "event executor terminated" with multiple workers in distributed mode
- [improvement] KAF-31: Support DSE DateRange Type
- [improvement] KAF-13: Exclude unnecessary classes from uberjar
- [bug] KAF-32: Connector is unkillable after encountering a connectivity issue with DSE
- [bug] KAF-33: Connector loses records when DSE connection is lost
- [bug] KAF-34: Package a simple README.md in the Kafka connector bundle
- [bug] KAF-35: Connector causes spurious DEBUG logging in Confluent 3.2.1 connect infrastructure
