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
package com.datastax.oss.sink.kafka.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class JsonEndToEndCCMIT extends EndToEndCCMITBase {

  public JsonEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void raw_udt_value_from_json() {
    conn.start(makeConnectorProperties("bigintcol=key, udtcol=value"));

    SinkRecord record =
        new SinkRecord(
            "mytopic",
            0,
            null,
            98761234L,
            null,
            "{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}",
            1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, udtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(session.getContext());
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));
  }

  @Test
  void raw_udt_value_and_cherry_pick_from_json() {
    conn.start(makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    SinkRecord record =
        new SinkRecord(
            "mytopic",
            0,
            null,
            98761234L,
            null,
            "{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}",
            1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, udtcol, intcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(session.getContext());
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));
    assertThat(row.getInt("intcol")).isEqualTo(42);
  }

  @Test
  void simple_json_value_only() {
    // Since the well-established JSON converter codecs do all the heavy lifting,
    // we don't test json very deeply here.
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "smallintcol=value.smallint, "
                + "textcol=value.text, "
                + "tinyintcol=value.tinyint"));

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

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getBoolean("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseValue + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getString("textcol")).isEqualTo(baseValue.toString());
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
  }

  @Test
  void complex_json_value_only() {
    conn.start(makeConnectorProperties("bigintcol=value.f1, mapcol=value.f2"));

    String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(42);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(2);
    assertThat(mapcol).containsEntry("sub1", 37).containsEntry("sub2", 96);
  }

  @Test
  void json_key_struct_value() {
    // Map various fields from the key and value to columns.
    conn.start(
        makeConnectorProperties(
            "bigintcol=key.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=key.double, "
                + "floatcol=value.float, "
                + "intcol=key.int, "
                + "smallintcol=value.smallint, "
                + "textcol=key.text, "
                + "tinyintcol=value.tinyint"));

    // Use a Struct for the value.
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("smallint", Schema.INT16_SCHEMA)
            .field("text", Schema.STRING_SCHEMA)
            .field("tinyint", Schema.INT8_SCHEMA)
            .build();
    Long baseValue = 98761234L;
    Struct structValue =
        new Struct(schema)
            .put("bigint", baseValue)
            .put("boolean", (baseValue.intValue() & 1) == 1)
            .put("double", (double) baseValue + 0.123)
            .put("float", baseValue.floatValue() + 0.987f)
            .put("int", baseValue.intValue())
            .put("smallint", baseValue.shortValue())
            .put("text", baseValue.toString())
            .put("tinyint", baseValue.byteValue());

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

    SinkRecord record = new SinkRecord("mytopic", 0, null, jsonKey, null, structValue, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseKey);
    assertThat(row.getBoolean("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseKey + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseKey.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getString("textcol")).isEqualTo(baseKey.toString());
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
  }

  @Test
  void null_in_json() {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    conn.start(makeConnectorProperties("bigintcol=value.bigint, textcol=value.text"));

    String json = "{\"bigint\": 1234567, \"text\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isEqualTo("got here");
  }

  @Test
  void update_counter_table() {
    session.execute(
        "CREATE TABLE IF NOT EXISTS mycounter "
            + "(c1 int, c2 int, c3 counter, c4 counter, PRIMARY KEY (c1, c2))");
    session.execute("TRUNCATE mycounter");
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.key",
            ImmutableMap.of(
                String.format("topic.ctr.%s.mycounter.mapping", keyspaceName),
                "c1=value.f1, c2=value.f2, c3=value.f3, c4=value.f4")));
    String value = "{" + "\"f1\": 1, " + "\"f2\": 2, " + "\"f3\": 3, " + "\"f4\": 4" + "}";
    SinkRecord record = new SinkRecord("ctr", 0, null, null, null, value, 1234L);

    // Insert the record twice; the counter columns should accrue.
    runTaskWithRecords(record);
    task.put(Collections.singletonList(record));

    // Verify...
    List<Row> results = session.execute("SELECT * FROM mycounter").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("c3")).isEqualTo(6);
    assertThat(row.getLong("c4")).isEqualTo(8);
  }

  @Test
  void timezone_and_locale_UNITS_SINCE_EPOCH() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.key, "
                + "datecol=value.vdate, "
                + "timecol=value.vtime, "
                + "secondscol=value.vseconds",
            ImmutableMap.<String, String>builder()
                .put("topic.mytopic.codec.timeZone", "Europe/Paris")
                .put("topic.mytopic.codec.locale", "fr_FR")
                .put("topic.mytopic.codec.date", "cccc, d MMMM uuuu")
                .put("topic.mytopic.codec.time", "HHmmssSSS")
                .put("topic.mytopic.codec.timestamp", "UNITS_SINCE_EPOCH")
                .put("topic.mytopic.codec.unit", "SECONDS")
                .build()));

    String value =
        "{\n"
            + "  \"key\": 4376,\n"
            + "  \"vdate\": \"vendredi, 9 mars 2018\",\n"
            + "  \"vtime\": 171232584,\n"
            + "  \"vseconds\": 1520611952\n"
            + "}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT datecol, timecol, secondscol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLocalDate("datecol")).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(row.getLocalTime("timecol")).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getInstant("secondscol")).isEqualTo(Instant.parse("2018-03-09T16:12:32Z"));
  }

  @Test
  void timezone_and_locale_ISO_ZONED_DATE_TIME() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.key, "
                + "datecol=value.vdate, "
                + "timecol=value.vtime, "
                + "timestampcol=value.vtimestamp",
            ImmutableMap.<String, String>builder()
                .put("topic.mytopic.codec.timeZone", "Europe/Paris")
                .put("topic.mytopic.codec.locale", "fr_FR")
                .put("topic.mytopic.codec.date", "cccc, d MMMM uuuu")
                .put("topic.mytopic.codec.time", "HHmmssSSS")
                .put("topic.mytopic.codec.timestamp", "ISO_ZONED_DATE_TIME")
                .put("topic.mytopic.codec.unit", "SECONDS")
                .build()));

    String value =
        "{\n"
            + "  \"key\": 4376,\n"
            + "  \"vdate\": \"vendredi, 9 mars 2018\",\n"
            + "  \"vtime\": 171232584,\n"
            + "  \"vtimestamp\": \"2018-03-09T17:12:32.584+01:00[Europe/Paris]\"\n"
            + "}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT datecol, timecol, timestampcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLocalDate("datecol")).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(row.getLocalTime("timecol")).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getInstant("timestampcol")).isEqualTo(Instant.parse("2018-03-09T16:12:32.584Z"));
  }
}
