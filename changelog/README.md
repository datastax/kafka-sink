## Changelog


### 1.0.0 (in progress)
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
