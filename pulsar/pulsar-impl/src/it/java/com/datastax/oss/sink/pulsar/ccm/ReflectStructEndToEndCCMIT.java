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

import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.*;
import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

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
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.pulsar.ReflectionGenericRecordSink;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class ReflectStructEndToEndCCMIT
    extends EndToEndCCMITBase<org.apache.pulsar.client.api.schema.GenericRecord> {

  ReflectStructEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new ReflectionGenericRecordSink());
  }

  @Test
  void struct_value_only() throws ParseException {
    // We skip testing the following datatypes, since in Kafka messages, values for these
    // types would simply be strings or numbers, and we'd just pass these right through to
    // the ExtendedCodecRegistry for encoding:
    //
    // ascii
    // date
    // decimal
    // duration
    // inet
    // time
    // timestamp
    // timeuuid
    // uuid
    // varint

    String withDateRange = hasDateRange ? "daterangecol=value.daterange, " : "";
    String withGeotypes =
        ccm.getClusterType() == DSE
            ? "pointcol=value.point, linestringcol=value.linestring, polygoncol=value.polygon, "
            : "";

    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "smallintcol=value.smallint, "
                + "textcol=value.text, "
                + "tinyintcol=value.tinyint, "
                + "mapcol=value.map, "
                + "mapnestedcol=value.mapnested, "
                + "listcol=value.list, "
                + "listnestedcol=value.listnested, "
                + "setcol=value.set, "
                + "setnestedcol=value.setnested, "
                + "tuplecol=value.tuple, "
                + "udtcol=value.udt, "
                + "udtfromlistcol=value.udtfromlist, "
                + "booleanudtcol=value.booleanudt, "
                + "booleanudtfromlistcol=value.booleanudtfromlist, "
                + withGeotypes
                + withDateRange
                + "blobcol=value.blob"));

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
            .name("map")
            .type()
            .map()
            .values()
            .intType()
            .noDefault()
            .name("mapnested")
            .type()
            .map()
            .values(SchemaBuilder.map().values().stringType())
            .noDefault()
            .name("list")
            .type()
            .array()
            .items()
            .intType()
            .noDefault()
            .name("listnested")
            .type()
            .array()
            .items(SchemaBuilder.array().items().intType())
            .noDefault()
            .name("set")
            .type()
            .array()
            .items()
            .intType()
            .noDefault()
            .name("setnested")
            .type()
            .array()
            .items(SchemaBuilder.array().items().intType())
            .noDefault()
            .name("tuple")
            .type()
            .array()
            .items()
            .intType()
            .noDefault()
            .name("udt")
            .type()
            .map()
            .values()
            .intType()
            .noDefault()
            .name("udtfromlist")
            .type()
            .array()
            .items()
            .intType()
            .noDefault()
            .name("booleanudt")
            .type()
            .map()
            .values()
            .booleanType()
            .noDefault()
            .name("booleanudtfromlist")
            .type()
            .array()
            .items()
            .booleanType()
            .noDefault()
            .requiredBytes("blob")
            .requiredString("point")
            .requiredString("linestring")
            .requiredString("polygon")
            .requiredString("daterange")
            .endRecord();

    Map<String, Integer> mapValue =
        ImmutableMap.<String, Integer>builder().put("sub1", 37).put("sub2", 96).build();

    Map<String, Map<Integer, String>> nestedMapValue =
        ImmutableMap.<String, Map<Integer, String>>builder()
            .put(
                "sub1",
                ImmutableMap.<Integer, String>builder()
                    .put(37, "sub1sub1")
                    .put(96, "sub1sub2")
                    .build())
            .put(
                "sub2",
                ImmutableMap.<Integer, String>builder()
                    .put(47, "sub2sub1")
                    .put(90, "sub2sub2")
                    .build())
            .build();

    List<Integer> listValue = Arrays.asList(37, 96, 90);

    List<Integer> list2 = Arrays.asList(3, 2);
    List<List<Integer>> nestedListValue = Arrays.asList(listValue, list2);

    Map<String, Integer> udtValue =
        ImmutableMap.<String, Integer>builder().put("udtmem1", 47).put("udtmem2", 90).build();

    Map<String, Boolean> booleanUdtValue =
        ImmutableMap.<String, Boolean>builder().put("udtmem1", true).put("udtmem2", false).build();

    byte[] blobValue = new byte[] {12, 22, 32};

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
    value.put("map", mapValue);
    value.put("mapnested", nestedMapValue);
    value.put("list", listValue);
    value.put("listnested", nestedListValue);
    value.put("set", listValue);
    value.put("setnested", nestedListValue);
    value.put("tuple", listValue);
    value.put("udt", udtValue);
    value.put("udtfromlist", udtValue.values());
    value.put("booleanudt", booleanUdtValue);
    value.put("booleanudtfromlist", booleanUdtValue.values());
    value.put("blob", ByteBuffer.wrap(blobValue));
    value.put("point", "POINT (32.0 64.0)");
    value.put("linestring", "LINESTRING (32.0 64.0, 48.5 96.5)");
    value.put("polygon", "POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0))");
    value.put("daterange", "[* TO 2014-12-01]");

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value), 1234));

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
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getMap("mapnestedcol", String.class, Map.class)).isEqualTo(nestedMapValue);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(listValue);
    assertThat(row.getList("listnestedcol", Set.class))
        .isEqualTo(
            new ArrayList<Set>(Arrays.asList(new HashSet<>(listValue), new HashSet<>(list2))));
    assertThat(row.getSet("setcol", Integer.class)).isEqualTo(new HashSet<>(listValue));
    assertThat(row.getSet("setnestedcol", List.class)).isEqualTo(new HashSet<>(nestedListValue));

    DefaultTupleType tupleType =
        new DefaultTupleType(
            ImmutableList.of(DataTypes.SMALLINT, DataTypes.INT, DataTypes.INT),
            session.getContext());
    assertThat(row.getTupleValue("tuplecol")).isEqualTo(tupleType.newValue((short) 37, 96, 90));

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(session.getContext());
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(47, "90"));
    assertThat(row.getUdtValue("udtfromlistcol")).isEqualTo(udt.newValue(47, "90"));

    UserDefinedType booleanUdt =
        new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
            .withField("udtmem1", DataTypes.BOOLEAN)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    booleanUdt.attach(session.getContext());
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "false"));
    assertThat(row.getUdtValue("booleanudtfromlistcol"))
        .isEqualTo(booleanUdt.newValue(true, "false"));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "udtcol=value.struct, "
                + "booleanudtcol=value.booleanstruct"));

    Schema fieldSchema =
        SchemaBuilder.record("fieldSchema")
            .fields()
            .requiredInt("udtmem1")
            .requiredString("udtmem2")
            .endRecord();
    GenericRecord fieldValue = new GenericData.Record(fieldSchema);
    fieldValue.put("udtmem1", 42);
    fieldValue.put("udtmem2", "the answer");

    Schema booleanFieldSchema =
        SchemaBuilder.record("booleanFieldSchema")
            .fields()
            .requiredBoolean("udtmem1")
            .requiredString("udtmem2")
            .endRecord();
    GenericRecord booleanFieldValue = new GenericData.Record(booleanFieldSchema);
    booleanFieldValue.put("udtmem1", true);
    booleanFieldValue.put("udtmem2", "the answer");

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .name("struct")
            .type(fieldSchema)
            .noDefault()
            .name("booleanstruct")
            .type(booleanFieldSchema)
            .noDefault()
            .endRecord();

    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("struct", fieldValue);
    value.put("booleanstruct", booleanFieldValue);

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value), 1234));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, intcol=value.int, smallintcol=value.smallint"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .optionalLong("bigint")
            .optionalBoolean("boolean")
            .optionalDouble("double")
            .optionalFloat("float")
            .optionalInt("int")
            .optionalInt("smallint")
            .optionalString("text")
            .optionalInt("tinyint")
            .optionalBytes("blob")
            .endRecord();

    Long baseValue = 98761234L;
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", baseValue);
    value.put("int", baseValue.intValue());

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value), 1234));

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
  }

  @Test
  void struct_optional_fields_with_values() {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "smallintcol=value.smallint, "
                + "textcol=value.text, "
                + "tinyintcol=value.tinyint, "
                + "blobcol=value.blob"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .optionalLong("bigint")
            .optionalBoolean("boolean")
            .optionalDouble("double")
            .optionalFloat("float")
            .optionalInt("int")
            .optionalInt("smallint")
            .optionalString("text")
            .optionalInt("tinyint")
            .optionalBytes("blob")
            .endRecord();

    byte[] blobValue = new byte[] {12, 22, 32};

    Long baseValue = 98761234L;
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", baseValue);
    value.put("boolean", (baseValue.intValue() & 1) == 1);
    value.put("double", (double) baseValue + 0.123);
    value.put("float", baseValue.floatValue() + 0.987f);
    value.put("int", baseValue.intValue());
    value.put("smallint", (int) baseValue.shortValue());
    value.put("text", baseValue.toString());
    value.put("tinyint", (int) baseValue.byteValue());
    value.put("blob", ByteBuffer.wrap(blobValue));

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value), 1234));

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
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
  }

  @Test
  void raw_udt_value_from_struct() {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, udtcol=value"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredInt("udtmem1")
            .requiredString("udtmem2")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("udtmem1", 42);
    value.put("udtmem2", "the answer");

    sendRecord(mockRecord("mytopic", String.valueOf(98761234L), pulsarGenericRecord(value), 1234));

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
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredInt("udtmem1")
            .requiredString("udtmem2")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("udtmem1", 42);
    value.put("udtmem2", "the answer");

    sendRecord(mockRecord("mytopic", String.valueOf(98761234L), pulsarGenericRecord(value), 1234));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.yourtopic.%s.types.mapping", keyspaceName),
                "bigintcol=key, intcol=value.value")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredInt("bigint")
            .requiredDouble("double")
            .endRecord();
    GenericRecord value1 = new GenericData.Record(schema);
    value1.put("bigint", 1234567L);
    value1.put("double", 42.0);
    GenericRecord value2 = new GenericData.Record(schema);
    value2.put("bigint", 9876543L);
    value2.put("double", 21.0);

    Schema schema2 = SchemaBuilder.record("your").fields().requiredInt("value").endRecord();
    GenericRecord value3 = new GenericData.Record(schema2);
    value3.put("value", 3333);

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value1), 1234));
    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value2), 1235));
    sendRecord(mockRecord("yourtopic", "5555", pulsarGenericRecord(value3), 1235));

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
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.mapping", keyspaceName),
                "bigintcol=value.bigint, intcol=value.int")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredBoolean("boolean")
            .requiredInt("int")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("boolean", true);
    value.put("int", 5725);

    sendRecord(mockRecord("mytopic", null, pulsarGenericRecord(value), 1234));

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

  /** Test for KAF-83 (case-sensitive fields and columns). */
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
    initConnectorAndTask(
        makeConnectorProperties(
            "\"bigint col\" = \"key.bigint_field\", "
                + "\"boolean-col\" = \"key.boolean-field\", "
                + "\"INT COL\" = \"value.INT_FIELD\", "
                + "\"TEXT.COL\" = \"value.TEXT_FIELD\"",
            "CASE_SENSITIVE",
            null));

    // Set up records for "mytopic"
    String key = "{\"bigint_field\":1234567,\"boolean-field\":true}";
    GenericRecord value =
        new GenericData.Record(
            SchemaBuilder.record("pulsar")
                .fields()
                .requiredInt("INT_FIELD")
                .requiredString("TEXT_FIELD")
                .endRecord());
    value.put("INT_FIELD", 5725);
    value.put("TEXT_FIELD", "foo");

    // Note: with the current mapping grammar, it is not possible to distinguish f1.f2 (i.e. a field
    // "f1" containing a nested field "f2") from a field named "f1.f2".

    sendRecord(mockRecord("mytopic", key, pulsarGenericRecord(value), 1234));

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
