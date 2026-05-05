📘 [Documentation](../README.md) > [Data Mapping](README.md) > JSON Data

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](../config/README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Mapping a message that contain JSON fields
 
For JSON fields, map individual fields in the structure to columns.
 When the data format for the key or value is JSON, the connector mapping can include individual
 fields in the JSON structure. DataStax Apache Kafka™ supports JSON produced by
 both the `JsonSerializer` and `StringSerializer`; mapping semantics
 are the same.
> **Note:** Note: JSON records in Kafka can also have a schema associated with them.
