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
package com.datastax.oss.sink.pulsar.ccm;

import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.GenSchemaGenericRecordSink;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class GenJsonEndToEndCCMIT
    extends EndToEndCCMITBase<org.apache.pulsar.client.api.schema.GenericRecord> {

  public GenJsonEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new GenSchemaGenericRecordSink());
  }

  @Test
  void raw_udt_value_from_json() {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, udtcol=value"));

    ;
    sendRecord(
        mockRecord(
            "mytopic",
            "98761234",
            pulsarGenericJsonRecord("{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}"),
            1234));

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
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    sendRecord(
        mockRecord(
            "mytopic",
            "98761234",
            pulsarGenericJsonRecord("{\"udtmem1\": 42, \"udtmem2\": \"the answer\"}"),
            1234));

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
    initConnectorAndTask(
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

    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(value), 1234));

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
    initConnectorAndTask(makeConnectorProperties("bigintcol=value.f1, mapcol=value.f2"));

    String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";
    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(value), 1234));

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
    initConnectorAndTask(
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

    sendRecord(mockRecord("mytopic", jsonKey, pulsarGenericAvroRecord(value), 1234));

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

    initConnectorAndTask(makeConnectorProperties("bigintcol=value.bigint, textcol=value.text"));

    String json = "{\"bigint\": 1234567, \"text\": null}";
    org.apache.pulsar.client.api.schema.GenericRecord rec = pulsarGenericJsonRecord(json);
    System.out.println(rec);
    sendRecord(mockRecord("mytopic", null, rec, 1234L));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.key",
            ImmutableMap.of(
                String.format("topic.ctr.%s.mycounter.mapping", keyspaceName),
                "c1=value.f1, c2=value.f2, c3=value.f3, c4=value.f4")));
    String value = "{" + "\"f1\": 1, " + "\"f2\": 2, " + "\"f3\": 3, " + "\"f4\": 4" + "}";

    // Insert the record twice; the counter columns should accrue.
    sendRecord(mockRecord("ctr", null, pulsarGenericJsonRecord(value), 1234L));
    sendRecord(mockRecord("ctr", null, pulsarGenericJsonRecord(value), 1234L));
    //    task.put(Collections.singletonList(record));

    // Verify...
    List<Row> results = session.execute("SELECT * FROM mycounter").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("c3")).isEqualTo(6);
    assertThat(row.getLong("c4")).isEqualTo(8);
  }

  @Test
  void timezone_and_locale_UNITS_SINCE_EPOCH() {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.key, "
                + "datecol=value.vdate, "
                + "timecol=value.vtime, "
                + "secondscol=value.vseconds",
            ImmutableMap.<String, Object>builder()
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
            + "  \"vtime\": \"171232584\",\n"
            + "  \"vseconds\": 1520611952\n"
            + "}";
    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(value), 1234L));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.key, "
                + "datecol=value.vdate, "
                + "timecol=value.vtime, "
                + "timestampcol=value.vtimestamp",
            ImmutableMap.<String, Object>builder()
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
            + "  \"vtime\": \"171232584\",\n"
            + "  \"vtimestamp\": \"2018-03-09T17:12:32.584+01:00[Europe/Paris]\"\n"
            + "}";
    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(value), 1234L));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT datecol, timecol, timestampcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLocalDate("datecol")).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(row.getLocalTime("timecol")).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getInstant("timestampcol")).isEqualTo(Instant.parse("2018-03-09T16:12:32.584Z"));
  }
}
