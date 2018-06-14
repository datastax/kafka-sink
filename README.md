# Kafka Sink for DataStax Enterprise

Kafka sink for transferring events/messages from Kafka topics to DSE

## How to use

First build the uber jar: `mvn clean package -DskipTests`

Copy the `target/kafka-connect-dse-*.jar` to the directory in your Kafka installation where connector
jars live. In Confluent, this is the `share/java` directory. For vanilla Kafka, it is likely the
`libs` directory.

Edit the `conf/dse-sink.properties` config file in this project to meet your needs,
or copy it out and edit elsewhere.

Run connect-standalone and specify the path to the config file:

`bin/connect-standalone.sh config/connect-standalone.properties ~/kafka-sink/conf/dse-sink.properties`

In Confluent, you would do this:

`bin/confluent load dse-sink -d ~/kafka-sink/conf/dse-sink.properties`

## Mapping specification

At this time, the connector supports Struct's with fields of the following types:
bigint, boolean, double, float, int, smallint, text, tinyint.

If you're using the InfiniteStreamSource from the kafka-examples project to publish
messages, you can use the following mapping spec to insert into a DSE table.
`bigintcol=value.bigint, booleancol=value.boolean, doublecol=value.double, floatcol=value.float, intcol=value.int, smallintcol=value.smallint, textcol=value.text, tinyintcol=value.tinyint`

Create the table like this:
```
create table simplex.types (
  bigintcol bigint PRIMARY KEY,
  booleancol boolean,
  doublecol double,
  floatcol float,
  intcol int,
  smallintcol smallint,
  textcol text,
  tinyIntcol tinyint
)
```

**NOTE: only happy path works right now. Most error conditions will likely cause ugly 
stack traces and crashes.**