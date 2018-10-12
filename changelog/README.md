## Changelog

### 1.0.0 (in progress)
- [improvement] KAF-54: Change default-properties to be more intuitive
- [new feature] KAF-49: Add queryExecutionTimeout global setting
- [improvement] KAF-52: Refactor kafka-sink repo into a few Maven modules

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
