/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.api.core.type.geometry.LineString;
import com.datastax.dse.driver.api.core.type.geometry.Point;
import com.datastax.dse.driver.api.core.type.geometry.Polygon;
import com.datastax.dse.driver.internal.core.type.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.type.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.type.geometry.DefaultPolygon;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
class SimpleEndToEndCCMIT extends EndToEndCCMITBase {
  private AttachmentPoint attachmentPoint;
  private String keyspaceName;

  public SimpleEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
    assert session.getKeyspace().isPresent();
    keyspaceName = session.getKeyspace().get().asInternal();
    attachmentPoint =
        new AttachmentPoint() {
          @NotNull
          @Override
          public ProtocolVersion getProtocolVersion() {
            return session.getContext().getProtocolVersion();
          }

          @NotNull
          @Override
          public CodecRegistry getCodecRegistry() {
            return session.getContext().getCodecRegistry();
          }
        };
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

    String withDateRange = dse50 ? "" : ", daterangecol=value.daterange";
    conn.start(
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
                + "blobcol=value.blob, "
                + "pointcol=value.point, "
                + "linestringcol=value.linestring, "
                + "polygoncol=value.polygon"
                + withDateRange));

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
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field(
                "mapnested",
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                    .build())
            .field("list", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field(
                "listnested",
                SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
            .field("set", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field(
                "setnested",
                SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
            .field("tuple", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field("udt", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field("udtfromlist", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field(
                "booleanudt",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build())
            .field("booleanudtfromlist", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build())
            .field("blob", Schema.BYTES_SCHEMA)
            .field("point", Schema.STRING_SCHEMA)
            .field("linestring", Schema.STRING_SCHEMA)
            .field("polygon", Schema.STRING_SCHEMA)
            .field("daterange", Schema.STRING_SCHEMA)
            .build();

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
    Struct value =
        new Struct(schema)
            .put("bigint", baseValue)
            .put("boolean", (baseValue.intValue() & 1) == 1)
            .put("double", (double) baseValue + 0.123)
            .put("float", baseValue.floatValue() + 0.987f)
            .put("int", baseValue.intValue())
            .put("smallint", baseValue.shortValue())
            .put("text", baseValue.toString())
            .put("tinyint", baseValue.byteValue())
            .put("map", mapValue)
            .put("mapnested", nestedMapValue)
            .put("list", listValue)
            .put("listnested", nestedListValue)
            .put("set", listValue)
            .put("setnested", nestedListValue)
            .put("tuple", listValue)
            .put("udt", udtValue)
            .put("udtfromlist", udtValue.values())
            .put("booleanudt", booleanUdtValue)
            .put("booleanudtfromlist", booleanUdtValue.values())
            .put("blob", blobValue)
            .put("point", "POINT (32.0 64.0)")
            .put("linestring", "LINESTRING (32.0 64.0, 48.5 96.5)")
            .put("polygon", "POLYGON ((0.0 0.0, 20.0 0.0, 25.0 25.0, 0.0 25.0, 0.0 0.0))")
            .put("daterange", "[* TO 2014-12-01]");

    runTaskWithRecords(new SinkRecord("mytopic", 0, null, null, null, value, 1234L));

    // Verify that the record was inserted properly in DSE.
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
            ImmutableList.of(DataTypes.SMALLINT, DataTypes.INT, DataTypes.INT), attachmentPoint);
    assertThat(row.getTupleValue("tuplecol")).isEqualTo(tupleType.newValue((short) 37, 96, 90));

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(47, "90"));
    assertThat(row.getUdtValue("udtfromlistcol")).isEqualTo(udt.newValue(47, "90"));

    UserDefinedType booleanUdt =
        new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
            .withField("udtmem1", DataTypes.BOOLEAN)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    booleanUdt.attach(attachmentPoint);
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "false"));
    assertThat(row.getUdtValue("booleanudtfromlistcol"))
        .isEqualTo(booleanUdt.newValue(true, "false"));

    assertThat(row.getByteBuffer("blobcol").array()).isEqualTo(blobValue);
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
    if (!dse50) {
      assertThat(row.get("daterangecol", GenericType.of(DateRange.class)))
          .isEqualTo(DateRange.parse("[* TO 2014-12-01]"));
    }
  }

  @Test
  void struct_value_struct_field() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "udtcol=value.struct, "
                + "booleanudtcol=value.booleanstruct"));

    Schema fieldSchema =
        SchemaBuilder.struct()
            .field("udtmem1", Schema.INT32_SCHEMA)
            .field("udtmem2", Schema.STRING_SCHEMA)
            .build();
    Struct fieldValue = new Struct(fieldSchema).put("udtmem1", 42).put("udtmem2", "the answer");

    Schema booleanFieldSchema =
        SchemaBuilder.struct()
            .field("udtmem1", Schema.BOOLEAN_SCHEMA)
            .field("udtmem2", Schema.STRING_SCHEMA)
            .build();
    Struct booleanFieldValue =
        new Struct(booleanFieldSchema).put("udtmem1", true).put("udtmem2", "the answer");

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("struct", fieldSchema)
            .field("booleanstruct", booleanFieldSchema)
            .build();

    Struct value =
        new Struct(schema)
            .put("bigint", 1234567L)
            .put("struct", fieldValue)
            .put("booleanstruct", booleanFieldValue);

    runTaskWithRecords(new SinkRecord("mytopic", 0, null, null, null, value, 1234L));

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));

    UserDefinedType booleanUdt =
        new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
            .withField("udtmem1", DataTypes.BOOLEAN)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    booleanUdt.attach(attachmentPoint);
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "the answer"));
  }

  @Test
  void struct_optional_fields_missing() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, intcol=value.int, smallintcol=value.smallint"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
            .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("int", Schema.OPTIONAL_INT32_SCHEMA)
            .field("smallint", Schema.OPTIONAL_INT16_SCHEMA)
            .field("text", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tinyint", Schema.OPTIONAL_INT8_SCHEMA)
            .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
            .build();

    Long baseValue = 98761234L;
    Struct value = new Struct(schema).put("bigint", baseValue).put("int", baseValue.intValue());

    runTaskWithRecords(new SinkRecord("mytopic", 0, null, null, null, value, 1234L));

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
  }

  @Test
  void struct_optional_fields_with_values() {
    conn.start(
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
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
            .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("int", Schema.OPTIONAL_INT32_SCHEMA)
            .field("smallint", Schema.OPTIONAL_INT16_SCHEMA)
            .field("text", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tinyint", Schema.OPTIONAL_INT8_SCHEMA)
            .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
            .build();

    byte[] blobValue = new byte[] {12, 22, 32};

    Long baseValue = 98761234L;
    Struct value =
        new Struct(schema)
            .put("bigint", baseValue)
            .put("boolean", (baseValue.intValue() & 1) == 1)
            .put("double", (double) baseValue + 0.123)
            .put("float", baseValue.floatValue() + 0.987f)
            .put("int", baseValue.intValue())
            .put("smallint", baseValue.shortValue())
            .put("text", baseValue.toString())
            .put("tinyint", baseValue.byteValue())
            .put("blob", blobValue);

    runTaskWithRecords(new SinkRecord("mytopic", 0, null, null, null, value, 1234L));

    // Verify that the record was inserted properly in DSE.
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
    assertThat(row.getByteBuffer("blobcol").array()).isEqualTo(blobValue);
  }

  @Test
  void struct_optional_field_with_default_value() {
    conn.start(makeConnectorProperties("bigintcol=value.bigint, intcol=value.int"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
            .field("int", SchemaBuilder.int32().optional().defaultValue(42).build())
            .build();

    Long baseValue = 98761234L;
    Struct value = new Struct(schema).put("bigint", baseValue);

    runTaskWithRecords(new SinkRecord("mytopic", 0, null, null, null, value, 1234L));

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue);
    assertThat(row.getInt("intcol")).isEqualTo(42);
  }

  @Test
  void raw_bigint_value() {
    conn.start(makeConnectorProperties("bigintcol=value"));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_string_value() {
    conn.start(makeConnectorProperties("bigintcol=key, pointcol=value"));

    SinkRecord record =
        new SinkRecord("mytopic", 0, null, 98761234L, null, "POINT (32.0 64.0)", 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, pointcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.get("pointcol", GenericType.of(Point.class)))
        .isEqualTo(new DefaultPoint(32.0, 64.0));
  }

  @Test
  void raw_byte_array_value() {
    conn.start(makeConnectorProperties("bigintcol=key, blobcol=value"));

    byte[] bytes = new byte[] {1, 2, 3};
    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, bytes, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, blobcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getByteBuffer("blobcol").array()).isEqualTo(bytes);
  }

  @Test
  void raw_list_value_from_json() {
    conn.start(makeConnectorProperties("bigintcol=key, listcol=value"));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, "[42, 37]", 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }

  @Test
  void raw_list_value_from_list() {
    conn.start(makeConnectorProperties("bigintcol=key, listcol=value"));

    SinkRecord record =
        new SinkRecord("mytopic", 0, null, 98761234L, null, Arrays.asList(42, 37), 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
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

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, udtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
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

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, udtcol, intcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));
    assertThat(row.getInt("intcol")).isEqualTo(42);
  }

  @Test
  void raw_udt_value_from_struct() {
    conn.start(makeConnectorProperties("bigintcol=key, udtcol=value"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("udtmem1", Schema.INT32_SCHEMA)
            .field("udtmem2", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("udtmem1", 42).put("udtmem2", "the answer");

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, udtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
    assertThat(row.getUdtValue("udtcol")).isEqualTo(udt.newValue(42, "the answer"));
  }

  @Test
  void raw_udt_value_and_cherry_pick_from_struct() {
    conn.start(makeConnectorProperties("bigintcol=key, udtcol=value, intcol=value.udtmem1"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("udtmem1", Schema.INT32_SCHEMA)
            .field("udtmem2", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("udtmem1", 42).put("udtmem2", "the answer");

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, udtcol, intcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    udt.attach(attachmentPoint);
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

    // Verify that the record was inserted properly in DSE.
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

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(42);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
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

    // Verify that the record was inserted properly in DSE.
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
  void timestamp() {
    conn.start(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(153000987000L);
  }

  @Test
  void null_to_unset_true() {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    conn.start(makeConnectorProperties("bigintcol=key, textcol=value"));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 1234567L, null, null, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE; textcol should be unchanged.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isEqualTo("got here");
  }

  @Test
  void null_to_unset_false() {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    conn.start(
        makeConnectorProperties(
            "bigintcol=key, textcol=value",
            ImmutableMap.<String, String>builder()
                .put("topic.mytopic.nullToUnset", "false")
                .build()));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 1234567L, null, null, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE; textcol should be unchanged.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isNull();
  }

  @Test
  void null_in_json() {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    conn.start(makeConnectorProperties("bigintcol=value.bigint, textcol=value.text"));

    String json = "{\"bigint\": 1234567, \"text\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE; textcol should be unchanged.
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
                "topic.ctr.keyspace", keyspaceName,
                "topic.ctr.table", "mycounter",
                "topic.ctr.mapping", "c1=value.f1, c2=value.f2, c3=value.f3, c4=value.f4")));
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
  void timezone_and_locale() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.key, "
                + "datecol=value.vdate, "
                + "timecol=value.vtime, "
                + "timestampcol=value.vtimestamp, "
                + "secondscol=value.vseconds",
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
            + "  \"vtimestamp\": \"2018-03-09T17:12:32.584+01:00[Europe/Paris]\",\n"
            + "  \"vseconds\": 1520611952\n"
            + "}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT datecol, timecol, timestampcol, secondscol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLocalDate("datecol")).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(row.getLocalTime("timecol")).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getInstant("timestampcol")).isEqualTo(Instant.parse("2018-03-09T16:12:32.584Z"));
    assertThat(row.getInstant("secondscol")).isEqualTo(Instant.parse("2018-03-09T16:12:32Z"));
  }

  @Test
  void multiple_records_multiple_topics() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double", "bigintcol=key, intcol=value"));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .build();
    Struct value1 = new Struct(schema).put("bigint", 1234567L).put("double", 42.0);
    Struct value2 = new Struct(schema).put("bigint", 9876543L).put("double", 21.0);

    SinkRecord record1 = new SinkRecord("mytopic", 0, null, null, null, value1, 1234L);
    SinkRecord record2 = new SinkRecord("mytopic", 0, null, null, null, value2, 1235L);

    // Set up a record for "yourtopic"
    SinkRecord record3 = new SinkRecord("yourtopic", 0, null, 5555L, null, 3333, 1000L);

    runTaskWithRecords(record1, record2, record3);

    // Verify that the record was inserted properly in DSE.
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

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }

  private Map<String, String> makeConnectorProperties(String mappingString) {
    return makeConnectorProperties(mappingString, "bigintcol=value.f1", null);
  }

  @SuppressWarnings("SameParameterValue")
  private Map<String, String> makeConnectorProperties(
      String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "bigintcol=value.f1", extras);
  }

  @SuppressWarnings("SameParameterValue")
  private Map<String, String> makeConnectorProperties(
      String mappingString, String yourMappingString) {
    return makeConnectorProperties(mappingString, yourMappingString, null);
  }

  private Map<String, String> makeConnectorProperties(
      String myTopicMappingString, String yourTopicMappingString, Map<String, String> extras) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("name", "myinstance")
            .put(
                "contactPoints",
                ccm.getInitialContactPoints()
                    .stream()
                    .map(addr -> String.format("%s", addr.getHostAddress()))
                    .collect(Collectors.joining(",")))
            .put("port", String.format("%d", ccm.getBinaryPort()))
            .put("loadBalancing.localDc", "Cassandra")
            .put(
                "topic.mytopic.keyspace",
                session.getKeyspace().orElse(CqlIdentifier.fromInternal("UNKNOWN")).asCql(true))
            .put("topic.mytopic.table", "types")
            .put("topic.mytopic.mapping", myTopicMappingString)
            .put("topic.yourtopic.keyspace", session.getKeyspace().get().asCql(true))
            .put("topic.yourtopic.table", "types")
            .put("topic.yourtopic.mapping", yourTopicMappingString);

    if (extras != null) {
      builder.putAll(extras);
    }
    return builder.build();
  }
}
