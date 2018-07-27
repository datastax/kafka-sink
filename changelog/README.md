## Changelog


### 1.0.0 (in progress)
- [new feature] KAF-1: Support JSON Records
- [improvement] KAF-2: Replace duped dsbulk commons code with a ref to the new dsbulk-commons-1.2.0-ng
- [improvement] KAF-3: Expand Kafka Struct support to handle complex objects
- [new feature] KAF-4: Add support for remaining C* and DSE datatypes except Counter and DateRange
- [new feature] KAF-6: Support specifying TTL for inserted rows
- [new feature] KAF-25: Set timestamp on DSE row to the timestamp in the Kafka record
- [improvement] KAF-7: Support mapping raw values in records (e.g. mycol=value or mycol=key)