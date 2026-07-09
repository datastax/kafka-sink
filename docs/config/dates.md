📘 [Documentation](../README.md) > [Configuration](README.md) > Date and Time Conversion

---

**Quick Links:** [Home](../README.md) | [Install](../install/README.md) | [Config](README.md) | [Operations](../operations/README.md) | [Monitoring](../monitoring/README.md) | [FAQ](../faq.md) | [Troubleshooting](../troubleshooting/README.md)

---

# Converting date and times for a topic
 
Configure date and time conversion parameters for each topic.
 Use the following syntax to configure date and time conversion parameters for each
 topic.
```
topic.topic_name.codec.locale=en_US
topic.topic_name.codec.timeZone=UTC
topic.topic_name.codec.timestamp=CQL_TIMESTAMP
topic.topic_name.codec.date=ISO_LOCAL_DATE
topic.topic_name.codec.time=ISO_LOCAL_TIME
topic.topic_name.codec.unit=MILLISECONDS
```

 where topic_name is the name of the Apache Kafka™
 topic. 
## Parameters
 Configure the date and time settings to override the default. These settings apply
 per topic.
> **Note:** Note: Date and time settings support any public static field in
 `java.time.format.DateTimeFormatter` as an option.

**codec.timestamp**
: The temporal pattern to use for string-to-CQL timestamp
 conversion, the choices are:
- Date-time pattern such as `yyyy-MM-dd
 HH:mm:ss`.
- Pre-defined formatter such as
 `ISO_ZONED_DATE_TIME` or
 `ISO_INSTANT`.
- Special formatter `CQL_TIMESTAMP` parser that
 accepts all valid CQL literal formats for the timestamp
 type.
 
Default: `CQL_TIMESTAMP`

**codec.date**
: The temporal pattern to use for string-to-CQL date conversion. 
- Date-time pattern such as `yyyy-MM-dd`.
- Pre-defined formatter such as
 `ISO_LOCAL_DATE`.

Default: `ISO_LOCAL_DATE`

**codec.time**
: The temporal pattern to use for string-to-CQL time conversion. Valid choices:
- Date-time pattern, such as
 `HH:mm:ss`.
- Pre-defined formatter, such as
 `ISO_LOCAL_TIME`.

Default: `ISO_LOCAL_TIME`

**codec.unit**
: If the input is a string containing only digits that cannot be parsed
 using the `codec.timestamp` format, the specified time
 unit is applied to the parsed value. All TimeUnit enum constants are
 valid choices.
Default: `MILLISECONDS`

**codec.timeZone**
: The time zone to use for temporal conversions that do not convey any
 explicit time zone information.
 
Default: `UTC`

**codec.locale**
: Locale to use for locale-sensitive conversions.
 
Default: `en_US`
See [DateTimeFormatter documentation](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) for
 additional options. 
## Examples
 
Timezone example
 To write to a column with data type timestamp, for the test Kafka topic
 string field that contains
 `2018-03-09T17:12:32.584+01:00[Europe/Paris]`, use the
 setting:
 
```
topic.test.codec.timestamp=ISO_ZONED_DATE_TIME
```
 
Date example
 To write to a column with type date for a Kafka topic that contains a
 string field like "2018-04-12", use the setting:
 
```
topic.test.codec.date="yyyy-MM-dd"
```
 
Time example
 To write to a column with type time for a Kafka topic string field
 like "10:15:30", use
 setting:
```
topic.test.codec.time=ISO_LOCAL_TIME
```
