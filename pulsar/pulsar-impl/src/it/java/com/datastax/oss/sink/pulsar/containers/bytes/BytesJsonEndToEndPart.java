/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar.containers.bytes;

import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.oss.sink.util.Tuple2;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("containers")
public class BytesJsonEndToEndPart extends BytesPart {

  @Override
  protected String basicName() {
    return "bytes-json";
  }

  @AfterEach
  void teardown() {
    cassandraSession.execute("truncate types");
  }

  @Test
  void raw_udt_value_from_json() throws PulsarAdminException, PulsarClientException {
    String name = name("ruvfj");
    regSink(name, "types", "bigintcol=key, udtcol=value");

    send(name, "98761234", "{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}".getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, udtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UDTValue udt = row.getUDTValue("udtcol");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isEqualTo("the answer");
  }

  @Test
  void raw_udt_value_and_cherry_pick_from_json()
      throws PulsarAdminException, PulsarClientException {
    String name = name("ruvacpfj");
    regSink(name, "types", "bigintcol=key, udtcol=value, intcol=value.udtmem1");

    send(name, "98761234", "{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}".getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, udtcol, intcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UDTValue udt = row.getUDTValue("udtcol");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isEqualTo("the answer");
    assertThat(row.getInt("intcol")).isEqualTo(42);
  }

  @Test
  void simple_json_value_only() throws PulsarAdminException, PulsarClientException {
    String name = name("sjvo");
    // Since the well-established JSON converter codecs do all the heavy lifting,
    // we don't test json very deeply here.
    String mapping =
        "bigintcol=value.bigint, "
            + "booleancol=value.boolean, "
            + "doublecol=value.double, "
            + "floatcol=value.float, "
            + "intcol=value.int, "
            + "smallintcol=value.smallint, "
            + "textcol=value.text, "
            + "tinyintcol=value.tinyint";
    regSink(name, "types", mapping);

    Long baseValue = 1234567L;
    String value =
        String.format(
            "{\"bigint\": %d, "
                + "\"boolean\": %b, "
                + "\"double\": %f, "
                + "\"float\": %f, "
                + "\"int\": %d, "
                + "\"smallint\": %d, "
                + "\"text\": \"%s\", "
                + "\"tinyint\": %d}",
            baseValue,
            (baseValue.intValue() & 1) == 1,
            (double) baseValue + 0.123,
            baseValue.floatValue() + 0.987f,
            baseValue.intValue(),
            baseValue.shortValue(),
            baseValue.toString(),
            baseValue.byteValue());

    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getBool("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseValue + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getString("textcol")).isEqualTo(baseValue.toString());
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
  }

  @Test
  void complex_json_value_only() throws PulsarAdminException, PulsarClientException {
    String name = name("cjvo");
    regSink(name, "types", "bigintcol=value.f1, mapcol=value.f2");

    String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";
    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(42);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(2);
    assertThat(mapcol).containsEntry("sub1", 37).containsEntry("sub2", 96);
  }

  @Test
  void json_key_struct_value() throws PulsarAdminException, PulsarClientException {
    // Map various fields from the key and value to columns.
    String name = name("jksv");
    String mapping =
        "bigintcol=key.bigint, "
            + "booleancol=value.boolean, "
            + "doublecol=key.double, "
            + "floatcol=value.float, "
            + "intcol=key.int, "
            + "smallintcol=value.smallint, "
            + "textcol=key.text, "
            + "tinyintcol=value.tinyint";
    regSink(name, "types", mapping);

    // Use a Struct for the value.
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredBoolean("boolean")
            .requiredDouble("double")
            .requiredFloat("float")
            .requiredInt("int")
            .requiredInt("smallint")
            .requiredString("text")
            .requiredInt("tinyint")
            .endRecord();
    Long baseValue = 98761234L;
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", baseValue);
    value.put("boolean", (baseValue.intValue() & 1) == 1);
    value.put("double", (double) baseValue + 0.123);
    value.put("float", baseValue.floatValue() + 0.987f);
    value.put("int", baseValue.intValue());
    value.put("smallint", baseValue.shortValue());
    value.put("text", baseValue.toString());
    value.put("tinyint", baseValue.byteValue());

    // Use JSON for the key.
    Long baseKey = 1234567L;
    String jsonKey =
        String.format(
            "{\"bigint\": %d, "
                + "\"boolean\": %b, "
                + "\"double\": %f, "
                + "\"float\": %f, "
                + "\"int\": %d, "
                + "\"smallint\": %d, "
                + "\"text\": \"%s\", "
                + "\"tinyint\": %d}",
            baseKey,
            (baseKey.intValue() & 1) == 1,
            (double) baseKey + 0.123,
            baseKey.floatValue() + 0.987f,
            baseKey.intValue(),
            baseKey.shortValue(),
            baseKey.toString(),
            baseKey.byteValue());

    send(name, jsonKey, wornBytes(value));
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseKey);
    assertThat(row.getBool("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseKey + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseKey.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getString("textcol")).isEqualTo(baseKey.toString());
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
  }

  @Test
  void null_in_json() throws PulsarAdminException, PulsarClientException {
    String name = name("nij");
    // Make a row with some value for textcol to start with.
    cassandraSession.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");
    regSink(name, "types", "bigintcol=value.bigint, textcol=value.text");

    String json = "{\"bigint\": 1234567, \"text\": null}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isEqualTo("got here");
  }

  @Test
  void update_counter_table() throws PulsarAdminException, PulsarClientException {
    String name = name("uct");
    cassandraSession.execute(
        "CREATE TABLE IF NOT EXISTS mycounter "
            + "(c1 int, c2 int, c3 counter, c4 counter, PRIMARY KEY (c1, c2))");
    cassandraSession.execute("TRUNCATE mycounter");
    regSink(name, "mycounter", "c1=value.f1, c2=value.f2, c3=value.f3, c4=value.f4");
    String value = "{" + "\"f1\": 1, " + "\"f2\": 2, " + "\"f3\": 3, " + "\"f4\": 4" + "}";

    // Insert the record twice; the counter columns should accrue.
    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();
    producer.newMessage().value(value.getBytes()).send();
    producer.newMessage().value(value.getBytes()).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 2);
    producer.close();
    unregisterSink(name);
    //    task.put(Collections.singletonList(record));

    // Verify...
    List<Row> results = cassandraSession.execute("SELECT * FROM mycounter").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("c3")).isEqualTo(6);
    assertThat(row.getLong("c4")).isEqualTo(8);
  }

  @Test
  void timezone_and_locale_UNITS_SINCE_EPOCH() throws PulsarAdminException, PulsarClientException {
    String name = name("taluse");
    String mapping =
        "bigintcol=value.key, "
            + "datecol=value.vdate, "
            + "timecol=value.vtime, "
            + "secondscol=value.vseconds";
    regSink(
        name,
        "types",
        mapping,
        Tuple2.of("topic." + name + ".codec.timeZone", "Europe/Paris"),
        Tuple2.of("topic." + name + ".codec.locale", "fr_FR"),
        Tuple2.of("topic." + name + ".codec.date", "cccc, d MMMM uuuu"),
        Tuple2.of("topic." + name + ".codec.time", "HHmmssSSS"),
        Tuple2.of("topic." + name + ".codec.timestamp", "UNITS_SINCE_EPOCH"),
        Tuple2.of("topic." + name + ".codec.unit", "SECONDS"));

    String value =
        "{\n"
            + "  \"key\": 4376,\n"
            + "  \"vdate\": \"vendredi, 9 mars 2018\",\n"
            + "  \"vtime\": \"171232584\",\n"
            + "  \"vseconds\": 1520611952\n"
            + "}";
    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT datecol, timecol, secondscol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getDate("datecol").toString()).isEqualTo(LocalDate.of(2018, 3, 9).toString());
    assertThat(LocalTime.ofNanoOfDay(row.getTime("timecol")))
        .isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getTimestamp("secondscol"))
        .isEqualTo(Date.from(Instant.parse("2018-03-09T16:12:32Z")));
  }

  @Test
  void timezone_and_locale_ISO_ZONED_DATE_TIME()
      throws PulsarAdminException, PulsarClientException {
    String name = name("talizdt");
    String mapping =
        "bigintcol=value.key, "
            + "datecol=value.vdate, "
            + "timecol=value.vtime, "
            + "timestampcol=value.vtimestamp";
    regSink(
        name,
        "types",
        mapping,
        Tuple2.of("topic." + name + ".codec.timeZone", "Europe/Paris"),
        Tuple2.of("topic." + name + ".codec.locale", "fr_FR"),
        Tuple2.of("topic." + name + ".codec.date", "cccc, d MMMM uuuu"),
        Tuple2.of("topic." + name + ".codec.time", "HHmmssSSS"),
        Tuple2.of("topic." + name + ".codec.timestamp", "ISO_ZONED_DATE_TIME"),
        Tuple2.of("topic." + name + ".codec.unit", "SECONDS"));

    String value =
        "{\n"
            + "  \"key\": 4376,\n"
            + "  \"vdate\": \"vendredi, 9 mars 2018\",\n"
            + "  \"vtime\": \"171232584\",\n"
            + "  \"vtimestamp\": \"2018-03-09T17:12:32.584+01:00[Europe/Paris]\"\n"
            + "}";
    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT datecol, timecol, timestampcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getDate("datecol").toString()).isEqualTo(LocalDate.of(2018, 3, 9).toString());
    assertThat(LocalTime.ofNanoOfDay(row.getTime("timecol")))
        .isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getTimestamp("timestampcol"))
        .isEqualTo(Date.from(Instant.parse("2018-03-09T16:12:32.584Z")));
  }
}
