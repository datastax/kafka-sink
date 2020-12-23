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
package com.datastax.oss.pulsar.sink.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class JsonEndToEndCCMIT extends EndToEndCCMITBase {

  public JsonEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void raw_udt_value_from_json() {
    taskConfigs.add(makeConnectorProperties("bigintcol=key, udtcol=value"));
    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    Schema recordTypeUtd =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl().put("udtmem1", 42).put("udtmem2", "the answer"),
            recordTypeUtd);
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
    taskConfigs.add(makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    Schema recordTypeUtd =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl().put("udtmem1", 42).put("udtmem2", "the answer"),
            recordTypeUtd);
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
    taskConfigs.add(
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

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl()
                .put("bigint", baseValue)
                .put("boolean", (baseValue.intValue() & 1) == 1)
                .put("double", (double) baseValue + 0.123)
                .put("float", baseValue.floatValue() + 0.987f)
                .put("int", baseValue.intValue())
                .put("smallint", baseValue.shortValue())
                .put("text", baseValue.toString())
                .put("tinyint", baseValue.byteValue()),
            recordTypeJson);

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
    taskConfigs.add(makeConnectorProperties("bigintcol=value.f1, mapcol=value.f2"));

    String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("f1").type(SchemaType.INT64);
    builder.field("f2").type(SchemaType.STRING);

    Schema recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl().put("f1", 42).put("f2", "{\"sub1\": 37, \"sub2\": 96}"),
            recordType);
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
  void null_in_json() {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    taskConfigs.add(makeConnectorProperties("bigintcol=value.bigint, textcol=value.text"));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl().put("bigint", 1234567L).put("text", null),
            recordTypeJson);

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
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.key",
            ImmutableMap.of(
                String.format("topic.ctr.%s.mycounter.mapping", keyspaceName),
                "c1=value.f1, c2=value.f2, c3=value.f3, c4=value.f4")));
    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("f1").type(SchemaType.INT64);
    builder.field("f2").type(SchemaType.INT64);
    builder.field("f3").type(SchemaType.INT64);
    builder.field("f4").type(SchemaType.INT64);
    Schema recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tn/ns/ctr",
            null,
            new GenericRecordImpl().put("f1", 1L).put("f2", 2L).put("f3", 3L).put("f4", 4L),
            recordType,
            null // NO TIMESTAMP
            );

    // Insert the record twice; the counter columns should accrue.
    runTaskWithRecords(record, record);

    // Verify...
    List<Row> results = session.execute("SELECT * FROM mycounter").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("c3")).isEqualTo(6);
    assertThat(row.getLong("c4")).isEqualTo(8);
  }

  @Test
  void timezone_and_locale_UNITS_SINCE_EPOCH() {
    taskConfigs.add(
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

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("key").type(SchemaType.INT32);
    builder.field("vdate").type(SchemaType.STRING);
    builder.field("vtime").type(SchemaType.STRING);
    builder.field("vseconds").type(SchemaType.INT32);
    Schema recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            null,
            new GenericRecordImpl()
                .put("key", 4376)
                .put("vdate", "vendredi, 9 mars 2018")
                .put("vtime", "171232584")
                .put("vseconds", 1520611952),
            recordType);
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
    taskConfigs.add(
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

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("key").type(SchemaType.INT32);
    builder.field("vdate").type(SchemaType.STRING);
    builder.field("vtimestamp").type(SchemaType.STRING);
    builder.field("vtime").type(SchemaType.STRING);
    Schema recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            null,
            new GenericRecordImpl()
                .put("key", 4376)
                .put("vdate", "vendredi, 9 mars 2018")
                .put("vtime", "171232584")
                .put("vtimestamp", "2018-03-09T17:12:32.584+01:00[Europe/Paris]"),
            recordType);
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
