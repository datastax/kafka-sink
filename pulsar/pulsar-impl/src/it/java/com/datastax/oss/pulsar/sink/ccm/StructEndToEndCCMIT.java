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

import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.DSE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class StructEndToEndCCMIT extends EndToEndCCMITBase {

  StructEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void struct_value_only() throws ParseException {
    String withDateRange = hasDateRange ? "daterangecol=value.daterange, " : "";
    String withGeotypes =
        ccm.getClusterType() == DSE
            ? "pointcol=value.point, linestringcol=value.linestring, polygoncol=value.polygon, "
            : "";

    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "textcol=value.text, "
                + withGeotypes
                + withDateRange
                + "blobcol=value.blob"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("boolean").type(SchemaType.BOOLEAN);
    builder.field("double").type(SchemaType.DOUBLE);
    builder.field("float").type(SchemaType.FLOAT);
    builder.field("int").type(SchemaType.INT32);
    builder.field("text").type(SchemaType.STRING);
    builder.field("blob").type(SchemaType.BYTES);
    builder.field("point").type(SchemaType.STRING);
    builder.field("linestring").type(SchemaType.STRING);
    builder.field("polygon").type(SchemaType.STRING);
    builder.field("daterange").type(SchemaType.STRING);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    byte[] blobValue = new byte[] {12, 22, 32};

    Long baseValue = 98761234L;
    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", baseValue)
            .put("boolean", (baseValue.intValue() & 1) == 1)
            .put("double", (double) baseValue + 0.123)
            .put("float", baseValue.floatValue() + 0.987f)
            .put("int", baseValue.intValue())
            .put("text", baseValue.toString())
            .put("blob", blobValue)
            .put("point", "POINT (32.0 64.0)")
            .put("linestring", "LINESTRING (32.0 64.0, 48.5 96.5)")
            .put("polygon", "POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0))")
            .put("daterange", "[* TO 2014-12-01]");

    runTaskWithRecords(
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getBoolean("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseValue + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getString("textcol")).isEqualTo(baseValue.toString());
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
    if (ccm.getClusterType() == DSE) {
      assertThat(row.get("pointcol", GenericType.of(Point.class)))
          .isEqualTo(new DefaultPoint(32.0, 64.0));
      assertThat(row.get("linestringcol", GenericType.of(LineString.class)))
          .isEqualTo(
              new DefaultLineString(new DefaultPoint(32.0, 64.0), new DefaultPoint(48.5, 96.5)));
      assertThat(row.get("polygoncol", GenericType.of(Polygon.class)))
          .isEqualTo(
              new DefaultPolygon(
                  new DefaultPoint(0, 0),
                  new DefaultPoint(20, 0),
                  new DefaultPoint(25, 25),
                  new DefaultPoint(0, 25),
                  new DefaultPoint(0, 0)));
    }
    if (hasDateRange) {
      assertThat(row.get("daterangecol", GenericType.of(DateRange.class)))
          .isEqualTo(DateRange.parse("[* TO 2014-12-01]"));
    }
  }

  @Test
  void struct_value_struct_field() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "udtcol=value.struct, "
                + "booleanudtcol=value.booleanstruct"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBeanInt");
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    GenericSchema fieldSchema =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecordImpl fieldValue =
        new GenericRecordImpl().put("udtmem1", 42).put("udtmem2", "the answer");

    RecordSchemaBuilder builder2 =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBeanBoolean");
    builder2.field("udtmem1").type(SchemaType.BOOLEAN);
    builder2.field("udtmem2").type(SchemaType.STRING);
    GenericSchema booleanFieldSchema =
        org.apache.pulsar.client.api.Schema.generic(builder2.build(SchemaType.AVRO));

    GenericRecordImpl booleanFieldValue =
        new GenericRecordImpl().put("udtmem1", true).put("udtmem2", "the answer");

    RecordSchemaBuilder builderRoot =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBeanRoot");
    builderRoot.field("bigint").type(SchemaType.INT64);
    builderRoot.field("struct", fieldSchema).type(SchemaType.AVRO);
    builderRoot.field("booleanstruct", booleanFieldSchema).type(SchemaType.AVRO);
    GenericSchema schema =
        org.apache.pulsar.client.api.Schema.generic(builderRoot.build(SchemaType.AVRO));

    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", 1234567L)
            .put("struct", fieldValue)
            .put("booleanstruct", booleanFieldValue);

    runTaskWithRecords(
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(session.getContext());
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));

    UserDefinedType booleanUdt =
        new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
            .withField("udtmem1", DataTypes.BOOLEAN)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    booleanUdt.attach(session.getContext());
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "the answer"));
  }

  @Test
  void struct_optional_fields_missing() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, intcol=value.int, doublecol=value.double"));

    RecordSchemaBuilder builderRoot =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builderRoot.field("bigint").type(SchemaType.INT64).optional();
    builderRoot.field("boolean").type(SchemaType.BOOLEAN).optional();
    builderRoot.field("double").type(SchemaType.DOUBLE).optional();
    builderRoot.field("float").type(SchemaType.FLOAT).optional();
    builderRoot.field("int").type(SchemaType.INT32).optional();
    builderRoot.field("text").type(SchemaType.STRING).optional();
    builderRoot.field("blob").type(SchemaType.BYTES).optional();

    GenericSchema schema =
        org.apache.pulsar.client.api.Schema.generic(builderRoot.build(SchemaType.AVRO));

    Long baseValue = 98761234L;
    GenericRecordImpl value =
        new GenericRecordImpl().put("bigint", baseValue).put("int", baseValue.intValue());

    runTaskWithRecords(
        new PulsarRecordImpl("peristent://tenant/namespace/mytopic", null, value, schema));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
  }

  @Test
  void struct_optional_fields_with_values() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "textcol=value.text, "
                + "blobcol=value.blob"));

    RecordSchemaBuilder builderRoot =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builderRoot.field("bigint").type(SchemaType.INT64).optional();
    builderRoot.field("boolean").type(SchemaType.BOOLEAN).optional();
    builderRoot.field("double").type(SchemaType.DOUBLE).optional();
    builderRoot.field("float").type(SchemaType.FLOAT).optional();
    builderRoot.field("int").type(SchemaType.INT32).optional();
    builderRoot.field("text").type(SchemaType.STRING).optional();
    builderRoot.field("blob").type(SchemaType.BYTES).optional();

    GenericSchema schema =
        org.apache.pulsar.client.api.Schema.generic(builderRoot.build(SchemaType.AVRO));

    byte[] blobValue = new byte[] {12, 22, 32};

    Long baseValue = 98761234L;
    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", baseValue)
            .put("boolean", (baseValue.intValue() & 1) == 1)
            .put("double", (double) baseValue + 0.123)
            .put("float", baseValue.floatValue() + 0.987f)
            .put("int", baseValue.intValue())
            .put("smallint", baseValue.shortValue())
            .put("text", baseValue.toString())
            .put("blob", blobValue);

    runTaskWithRecords(
        new PulsarRecordImpl("peristent://tenant/namespace/mytopic", null, value, schema));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getBoolean("booleancol")).isEqualTo((baseValue.intValue() & 1) == 1);
    assertThat(row.getDouble("doublecol")).isEqualTo((double) baseValue + 0.123);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue() + 0.987f);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getString("textcol")).isEqualTo(baseValue.toString());
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
  }

  @Test
  void raw_udt_value_from_struct() {
    taskConfigs.add(makeConnectorProperties("bigintcol=key, udtcol=value"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    Schema recordTypeUtd =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));
    GenericRecordImpl value =
        new GenericRecordImpl().put("udtmem1", 42).put("udtmem2", "the answer");

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "98761234", value, recordTypeUtd);
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
  void raw_udt_value_and_cherry_pick_from_struct() {
    taskConfigs.add(makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    Schema recordTypeUtd =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));
    GenericRecordImpl value =
        new GenericRecordImpl().put("udtmem1", 42).put("udtmem2", "the answer");

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "98761234", value, recordTypeUtd);
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
  void multiple_records_multiple_topics() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.yourtopic.%s.types.mapping", keyspaceName),
                "bigintcol=key, intcol=value.int")));

    // Set up records for "mytopic"
    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("double").type(SchemaType.DOUBLE);
    Schema recordTypeUtd =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecordImpl value1 = new GenericRecordImpl().put("bigint", 1234567L).put("double", 42.0);
    GenericRecordImpl value2 = new GenericRecordImpl().put("bigint", 9876543L).put("double", 21.0);

    PulsarRecordImpl record1 =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value1, recordTypeUtd);
    PulsarRecordImpl record2 =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value2, recordTypeUtd);

    // Set up a record for "yourtopic"

    PulsarRecordImpl record3 =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/yourtopic",
            "5555",
            new GenericRecordImpl().put("int", 3333),
            recordType);

    runTaskWithRecords(record1, record2, record3);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, doublecol, intcol FROM types").all();
    assertThat(results.size()).isEqualTo(3);
    for (Row row : results) {
      if (row.getLong("bigintcol") == 1234567L) {
        assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
        assertThat(row.getObject("intcol")).isNull();
      } else if (row.getLong("bigintcol") == 9876543L) {
        assertThat(row.getDouble("doublecol")).isEqualTo(21.0);
        assertThat(row.getObject("intcol")).isNull();
      } else if (row.getLong("bigintcol") == 5555L) {
        assertThat(row.getObject("doublecol")).isNull();
        assertThat(row.getInt("intcol")).isEqualTo(3333);
      }
    }
  }

  @Test
  void single_record_multiple_tables() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.mapping", keyspaceName),
                "bigintcol=value.bigint, intcol=value.int")));

    // Set up records for "mytopic"

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("boolean").type(SchemaType.BOOLEAN);
    builder.field("int").type(SchemaType.INT32);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecordImpl value =
        new GenericRecordImpl().put("bigint", 1234567L).put("boolean", true).put("int", 5725);
    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema);

    runTaskWithRecords(record);

    // Verify that a record was inserted in each of small_simple and types tables.
    {
      List<Row> results = session.execute("SELECT * FROM small_simple").all();
      assertThat(results.size()).isEqualTo(1);
      Row row = results.get(0);
      assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
      assertThat(row.get("booleancol", GenericType.BOOLEAN)).isNull();
      assertThat(row.getInt("intcol")).isEqualTo(5725);
    }
    {
      List<Row> results = session.execute("SELECT * FROM types").all();
      assertThat(results.size()).isEqualTo(1);
      Row row = results.get(0);
      assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
      assertThat(row.getBoolean("booleancol")).isTrue();
      assertThat(row.getInt("intcol")).isEqualTo(5725);
    }
  }

  /**
   * Test for KAF-83 (case-sensitive fields and columns). In Pulsar we cannot have field names with
   * dots and spaces, so this test is less significant than the one for Kafka sink
   */
  @Test
  void single_map_quoted_fields_to_quoted_columns() {
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE \"CASE_SENSITIVE\" ("
                    + "\"bigint col\" bigint, "
                    + "\"boolean-col\" boolean, "
                    + "\"INT COL\" int,"
                    + "\"TEXT.COL\" text,"
                    + "PRIMARY KEY (\"bigint col\", \"boolean-col\")"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());
    taskConfigs.add(
        makeConnectorProperties(
            "\"bigint col\" = \"key.bigint field\", "
                + "\"boolean-col\" = \"key.boolean-field\", "
                + "\"INT COL\" = \"value.INTFIELD\", "
                + "\"TEXT.COL\" = \"value.TEXTFIELD\"",
            "CASE_SENSITIVE",
            null));

    // Set up records for "mytopic"

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("INTFIELD").type(SchemaType.INT64);
    builder.field("TEXTFIELD").type(SchemaType.STRING);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));

    String key = "{\"bigint field\":1234567,\"boolean-field\":true}";

    GenericRecordImpl value = new GenericRecordImpl().put("INTFIELD", 5725).put("TEXTFIELD", "foo");

    // Note: with the current mapping grammar, it is not possible to distinguish f1.f2 (i.e. a field
    // "f1" containing a nested field "f2") from a field named "f1.f2".

    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", key, value, schema);

    runTaskWithRecords(record);

    // Verify that a record was inserted
    List<Row> results = session.execute("SELECT * FROM \"CASE_SENSITIVE\"").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("\"bigint col\"")).isEqualTo(1234567L);
    assertThat(row.getBoolean("\"boolean-col\"")).isTrue();
    assertThat(row.getInt("\"INT COL\"")).isEqualTo(5725);
    assertThat(row.getString("\"TEXT.COL\"")).isEqualTo("foo");
  }
}
