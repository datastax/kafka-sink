/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.kafkaconnector.config.DseSinkConfig.withDriverPrefix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("medium")
class SimpleEndToEndCCMIT extends EndToEndCCMITBase {
  private AttachmentPoint attachmentPoint;

  public SimpleEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
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

  @BeforeAll
  void createSmallTables() {
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS small_simple ("
                    + "bigintCol bigint PRIMARY KEY, "
                    + "booleanCol boolean, "
                    + "intCol int"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS small_compound ("
                    + "bigintCol bigint, "
                    + "booleanCol boolean, "
                    + "intCol int,"
                    + "PRIMARY KEY (bigintcol, booleancol)"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS pk_value ("
                    + "my_pk bigint PRIMARY KEY,"
                    + "my_value boolean"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS pk_value_with_timeuuid ("
                    + "my_pk bigint PRIMARY KEY,"
                    + "my_value boolean,"
                    + "loaded_at timeuuid"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());
  }

  @BeforeEach
  void truncateTables() {
    session.execute("TRUNCATE small_simple");
    session.execute("TRUNCATE small_compound");
    session.execute("TRUNCATE pk_value");
    session.execute("TRUNCATE pk_value_with_timeuuid");
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
                + withGeotypes
                + withDateRange
                + "blobcol=value.blob"));

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
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
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
  void should_insert_from_topic_with_complex_name() {
    conn.start(
        makeConnectorProperties("bigintcol=value", "types", null, "this.is.complex_topic-name"));

    SinkRecord record =
        new SinkRecord("this.is.complex_topic-name", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_snappy() {
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    conn.start(
        makeConnectorProperties("bigintcol=value", ImmutableMap.of("compression", "Snappy")));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_lz4() {
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    conn.start(makeConnectorProperties("bigintcol=value", ImmutableMap.of("compression", "LZ4")));

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
    conn.start(makeConnectorProperties("bigintcol=key, textcol=value"));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, "my text", 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getString("textcol")).isEqualTo("my text");
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
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(bytes);
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
            ImmutableMap.of(
                String.format("topic.mytopic.%s.types.nullToUnset", keyspaceName), "false")));

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

    // Verify that the record was inserted properly in DSE.
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

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT datecol, timecol, timestampcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLocalDate("datecol")).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(row.getLocalTime("timecol")).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(row.getInstant("timestampcol")).isEqualTo(Instant.parse("2018-03-09T16:12:32.584Z"));
  }

  @Test
  void multiple_records_multiple_topics() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.yourtopic.%s.types.mapping", keyspaceName),
                "bigintcol=key, intcol=value")));

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

  @Test
  void single_record_multiple_tables() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.mapping", keyspaceName),
                "bigintcol=value.bigint, intcol=value.int")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("boolean", true).put("int", 5725);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

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

  @Test
  void delete_simple_key() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("my_pk", Schema.INT64_SCHEMA)
            .field("my_value", Schema.BOOLEAN_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("my_pk", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_value_null() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=key.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    Schema keySchema =
        SchemaBuilder.struct().name("Kafka").field("my_pk", Schema.INT64_SCHEMA).build();
    Struct key = new Struct(keySchema).put("my_pk", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_value_null_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=key.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String key = "{\"my_pk\": 1234567}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void insert_with_nulls_when_delete_disabled() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_simple",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.deletesEnabled", keyspaceName),
                "false")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was inserted into DSE with null non-pk values.
    List<Row> results = session.execute("SELECT * FROM small_simple").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.get("bigintcol", GenericType.LONG)).isEqualTo(1234567L);
    assertThat(row.get("booleancol", GenericType.BOOLEAN)).isNull();
    assertThat(row.get("intcol", GenericType.INTEGER)).isNull();
  }

  @Test
  void delete_compound_key() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("boolean", true);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_json() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    String json = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_value_null() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    Schema keySchema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("bigint", 1234567L).put("boolean", true);
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_value_null_json() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    String key = "{\"bigint\": 1234567, \"boolean\": true}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void map_only() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "smallintcol=value.smallint, "
                + "textcol=value.text,"
                + "mapnestedcol=value.mapnested,"
                + "mapcol=value.map,"
                + "tinyintcol=value.tinyint"));

    Long baseValue = 1234567L;

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

    Map<String, Object> value = new HashMap<>();
    value.put("bigint", baseValue);
    value.put("boolean", (baseValue.intValue() & 1) == 1);
    value.put("double", (double) baseValue + 0.123);
    value.put("float", baseValue.floatValue() + 0.987f);
    value.put("int", baseValue.intValue());
    value.put("smallint", baseValue.shortValue());
    value.put("text", baseValue.toString());
    value.put("map", mapValue);
    value.put("mapnested", nestedMapValue);
    value.put("tinyint", baseValue.byteValue());

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
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getMap("mapnestedcol", String.class, Map.class)).isEqualTo(nestedMapValue);
  }

  @Test
  void raw_udt_value_map() {
    // given
    conn.start(makeConnectorProperties("bigintcol=key, listudtcol=value"));

    Map<String, Object> value = new HashMap<>();
    value.put("a", 42);
    value.put("b", "the answer");
    value.put("c", Arrays.asList(1, 2, 3));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);

    // when
    runTaskWithRecords(record);

    // then
    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, listudtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "mycomplexudt")
            .withField("a", DataTypes.INT)
            .withField("b", DataTypes.TEXT)
            .withField("c", DataTypes.listOf(DataTypes.INT))
            .build();
    udt.attach(attachmentPoint);
    assertThat(row.getUdtValue("listudtcol"))
        .isEqualTo(udt.newValue(42, "the answer", Arrays.asList(1, 2, 3)));
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
    conn.start(
        makeConnectorProperties(
            "\"bigint col\" = \"key.bigint field\", "
                + "\"boolean-col\" = \"key.boolean-field\", "
                + "\"INT COL\" = \"value.INT FIELD\", "
                + "\"TEXT.COL\" = \"value.TEXT.FIELD\"",
            "CASE_SENSITIVE",
            null));

    // Set up records for "mytopic"
    Struct key =
        new Struct(
                SchemaBuilder.struct()
                    .name("Kafka")
                    .field("bigint field", Schema.INT64_SCHEMA)
                    .field("boolean-field", Schema.BOOLEAN_SCHEMA)
                    .build())
            .put("bigint field", 1234567L)
            .put("boolean-field", true);
    Struct value =
        new Struct(
                SchemaBuilder.struct()
                    .name("Kafka")
                    .field("INT FIELD", Schema.INT32_SCHEMA)
                    .field("TEXT.FIELD", Schema.STRING_SCHEMA)
                    .build())
            .put("INT FIELD", 5725)
            .put("TEXT.FIELD", "foo");

    // Note: with the current mapping grammar, it is not possible to distinguish f1.f2 (i.e. a field
    // "f1" containing a nested field "f2") from a field named "f1.f2".

    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, value, 1234L);

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

  /** Test for KAF-84. */
  @Test
  void raw_udt_value_map_case_sensitive() {
    // given
    session.execute(
        SimpleStatement.builder(
                "CREATE TYPE case_sensitive_udt (\"Field A\" int, \"Field-B\" text, \"Field.C\" list<int>)")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE \"CASE_SENSITIVE_UDT\" (pk bigint PRIMARY KEY, value frozen<case_sensitive_udt>)")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    conn.start(makeConnectorProperties("pk=key, value=value", "\"CASE_SENSITIVE_UDT\"", null));

    Map<String, Object> value = new HashMap<>();
    value.put("Field A", 42);
    value.put("Field-B", "the answer");
    value.put("Field.C", Arrays.asList(1, 2, 3));

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);

    // when
    runTaskWithRecords(record);

    // then
    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT pk, value FROM \"CASE_SENSITIVE_UDT\"").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("pk")).isEqualTo(98761234L);
    UdtValue udtValue = row.getUdtValue("value");
    assertThat(udtValue).isNotNull();
    assertThat(udtValue.getInt("\"Field A\"")).isEqualTo(42);
    assertThat(udtValue.getString("\"Field-B\"")).isEqualTo("the answer");
    assertThat(udtValue.getList("\"Field.C\"", Integer.class)).containsExactly(1, 2, 3);
  }

  /** Test for KAF-107. */
  @Test
  void should_insert_record_with_ttl_provided_via_mapping() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("ttlcol", Schema.INT64_SCHEMA)
            .build();
    Number ttlValue = 1_000_000L;
    Struct value =
        new Struct(schema)
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("ttlcol", ttlValue.longValue());

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), ttlValue);
  }

  /** Test for KAF-107. */
  @ParameterizedTest(name = "[{index}] schema={0}, ttlValue={1}, expectedTtlValue={2}")
  @MethodSource("ttlColProvider")
  void should_insert_record_with_ttl_provided_via_mapping_and_validate_ttl_of_table(
      Schema schema, Number ttlValue, Number expectedTtlValue) {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    Struct value =
        new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), expectedTtlValue);
  }

  @Test
  void should_extract_ttl_from_json_and_use_as_ttl_column() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), 1000);
  }

  @Test
  void should_insert_value_using_now_function_json() {
    // given
    conn.start(makeConnectorProperties("bigintcol=value.bigint, loaded_at=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results = session.execute("SELECT bigintcol, loaded_at FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.get("loaded_at", TypeCodecs.TIMEUUID)).isLessThanOrEqualTo(Uuids.timeBased());
  }

  @Test
  void should_insert_value_using_now_function_for_two_dse_columns() {
    // given
    conn.start(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results = session.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.get("loaded_at", TypeCodecs.TIMEUUID);
    UUID loadedAt2 = row.get("loaded_at2", TypeCodecs.TIMEUUID);
    // columns inserted using now() should have different TIMEUUID values
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void should_insert_value_using_now_function_avro() {
    conn.start(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    Schema schema =
        SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).build();
    Struct value = new Struct(schema).put("bigint", 1234567L);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.get("loaded_at", TypeCodecs.TIMEUUID);
    UUID loadedAt2 = row.get("loaded_at2", TypeCodecs.TIMEUUID);
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void delete_simple_key_json_when_using_now_function_in_mapping() {
    // First insert a row...
    session.execute(
        "INSERT INTO pk_value_with_timeuuid (my_pk, my_value, loaded_at) VALUES (1234567, true, now())");
    List<Row> results = session.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(1);

    // now() function call is ignored when null value is send - DELETE will be performed
    conn.start(
        makeConnectorProperties(
            "my_pk=value.my_pk, my_value=value.my_value, loaded_at=now()",
            "pk_value_with_timeuuid",
            null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void should_extract_ttl_and_timestamp_from_json_and_use_as_ttl_and_timestamp_columns() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MICROSECONDS")));

    // when
    String json =
        "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000, \"timestampcol\": 1000}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session
            .execute("SELECT bigintcol, doublecol, ttl(doublecol), writetime(doublecol) FROM types")
            .all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), 1000);
    assertThat(row.getLong(3)).isEqualTo(1000L);
  }

  @Test
  void should_extract_ttl_from_json_and_use_existing_column_as_ttl() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000000.0);
    assertTtl(row.getInt(2), 1000);
  }

  @Test
  void should_use_ttl_from_config_and_use_as_ttl() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttl", keyspaceName, "types"), "100")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000.0);
    assertTtl(row.getInt(2), 100);
  }

  /** Test for KAF-46. */
  @Test
  void should_extract_timestamp_from_json_and_use_existing_column_as_timestamp() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000.0);
    assertThat(row.getLong(2)).isEqualTo(1_000_000L);
  }

  /** Test for KAF-46. */
  @Test
  void should_insert_record_with_timestamp_provided_via_mapping() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestamp"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .build();
    Struct value =
        new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("timestamp", 12314L);

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
    assertThat(row.getLong(2)).isEqualTo(12314L);
  }

  /** Test for KAF-46. */
  @Test
  void should_extract_write_timestamp_from_json_and_use_as_write_time_column() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol"));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"timestampcol\": 1000}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(1000L);
  }

  /** Test for KAF-46. */
  @ParameterizedTest(name = "[{index}] schema={0}, timestampValue={1}, expectedTimestampValue={2}")
  @MethodSource("timestampColProvider")
  void should_insert_record_with_timestamp_provided_via_mapping_and_validate_timestamp_of_table(
      Schema schema, Number timestampValue, Number expectedTimestampValue) {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    Struct value =
        new Struct(schema)
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("timestampcol", timestampValue);

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
    assertThat(row.getLong(2)).isEqualTo(expectedTimestampValue.longValue());
  }

  /** Test for KAF-135 */
  @Test
  void should_load_settings_from_dse_reference_conf() {
    // given (connector mapping need to be defined)
    conn.start(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));
    initConnectorAndTask();

    // when
    InstanceState instanceState = task.getInstanceState();

    // then setting from dse-reference.conf should be defined
    assertThat(
            instanceState
                .getSession()
                .getContext()
                .getConfig()
                .getDefaultProfile()
                .getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE))
        .isGreaterThan(0);
  }

  /** Test for KAF-142 */
  @Test
  void should_use_values_from_header_in_mapping() {
    // values used in this test are random and irrelevant for the test
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=header.bigint,"
                + "doublecol=header.double,"
                + "textcol=header.text,"
                + "booleancol=header.boolean,"
                + "tinyintcol=header.tinyint,"
                + "blobcol=header.blob,"
                + "floatcol=header.float,"
                + "intcol=header.int,"
                + "smallintcol=header.smallint,"
                + "mapcol=header.map,"
                + "mapnestedcol=header.mapnested,"
                + "listcol=header.list,"
                + "listnestedcol=header.listnested,"
                + "setcol=header.set,"
                + "setnestedcol=header.setnested,"
                + "booleanudtcol=header.booleanudt"));

    Long baseValue = 1234567L;
    byte[] blobValue = new byte[] {12, 22, 32};
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
    Map<String, Boolean> booleanUdtValue =
        ImmutableMap.<String, Boolean>builder().put("udtmem1", true).put("udtmem2", false).build();

    Headers headers =
        new ConnectHeaders()
            .add("bigint", baseValue, SchemaBuilder.int64().build())
            .add("double", baseValue.doubleValue(), SchemaBuilder.float64().build())
            .addString("text", "value")
            .addBoolean("boolean", false)
            .addByte("tinyint", baseValue.byteValue())
            .add("blob", ByteBuffer.wrap(blobValue), Schema.BYTES_SCHEMA)
            .addFloat("float", baseValue.floatValue())
            .addInt("int", baseValue.intValue())
            .addShort("smallint", baseValue.shortValue())
            .addMap(
                "map",
                mapValue,
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .add(
                "mapnested",
                nestedMapValue,
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                    .build())
            .addList("list", listValue, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .add(
                "listnested",
                nestedListValue,
                SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
            .add("set", listValue, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .add(
                "setnested",
                nestedListValue,
                SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
            .add(
                "booleanudt",
                booleanUdtValue,
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build());

    String json = "{\"bigint\": 1234567}";
    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, json, 1234L, 1L, TimestampType.CREATE_TIME, headers);

    // when
    runTaskWithRecords(record);

    // then
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue.longValue());
    assertThat(row.getDouble("doublecol")).isEqualTo(baseValue.doubleValue());
    assertThat(row.getString("textcol")).isEqualTo("value");
    assertThat(row.getBoolean("booleancol")).isEqualTo(false);
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue());
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getMap("mapnestedcol", String.class, Map.class)).isEqualTo(nestedMapValue);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(listValue);
    assertThat(row.getList("listnestedcol", Set.class))
        .isEqualTo(
            new ArrayList<Set>(Arrays.asList(new HashSet<>(listValue), new HashSet<>(list2))));
    assertThat(row.getSet("setcol", Integer.class)).isEqualTo(new HashSet<>(listValue));
    assertThat(row.getSet("setnestedcol", List.class)).isEqualTo(new HashSet<>(nestedListValue));
    UserDefinedType booleanUdt =
        new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
            .withField("udtmem1", DataTypes.BOOLEAN)
            .withField("udtmem2", DataTypes.TEXT)
            .build();
    booleanUdt.attach(attachmentPoint);
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "false"));
  }

  /** Test for KAF-142 */
  @Test
  void should_use_header_values_as_ttl_and_timestamp() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=header.bigint, doublecol=header.double, __ttl=header.ttlcolumn, __timestamp = header.timestampcolumn",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    long ttlValue = 1_000_000;
    Headers headers =
        new ConnectHeaders()
            .add("bigint", 100000L, SchemaBuilder.int64().build())
            .addLong("ttlcolumn", ttlValue)
            .addLong("timestampcolumn", 2345678L)
            .addDouble("double", 100L);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, null, 1234L, 1L, TimestampType.CREATE_TIME, headers);

    // when
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session
            .execute("SELECT bigintcol, doublecol, ttl(doublecol), writetime(doublecol) FROM types")
            .all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(100000L);
    assertThat(row.getDouble("doublecol")).isEqualTo(100);
    assertTtl(row.getInt(2), ttlValue);
    assertThat(row.getLong(3)).isEqualTo(2345678000L);
  }

  @Test
  void should_delete_when_header_values_are_null() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=header.my_pk, my_value=header.my_value", "pk_value", null));

    Headers headers =
        new ConnectHeaders()
            .add("my_pk", 1234567L, Schema.INT64_SCHEMA)
            .add("my_value", null, Schema.OPTIONAL_BOOLEAN_SCHEMA);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, null, 1234L, 1L, TimestampType.CREATE_TIME, headers);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void should_fail_when_using_header_without_specific_field_in_a_mapping() {
    conn.start(makeConnectorProperties("bigintcol=key, udtcol=header"));

    SinkRecord record =
        new SinkRecord(
            "mytopic",
            0,
            null,
            null,
            null,
            null,
            1234L,
            1L,
            TimestampType.CREATE_TIME,
            new ConnectHeaders());

    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid field name 'header': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.' or 'header.', or be one of supported functions: '[now()]'");
  }

  @Test
  void should_insert_when_using_java_driver_contact_points_setting() {
    Map<String, String> connectorProperties =
        makeConnectorPropertiesWithoutContactPointsAndPort("bigintcol=key, listcol=value");
    // use single datastax-java-driver prefixed property that carry host:port
    connectorProperties.put(
        withDriverPrefix(DefaultDriverOption.CONTACT_POINTS),
        getContactPoints()
            .stream()
            .map(
                a -> {
                  InetSocketAddress inetSocketAddress = (InetSocketAddress) a.resolve();
                  return String.format(
                      "%s:%d", inetSocketAddress.getHostString(), inetSocketAddress.getPort());
                })
            .collect(Collectors.joining(",")));

    conn.start(connectorProperties);

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, "[42, 37]", 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }

  private static Stream<? extends Arguments> ttlColProvider() {
    Supplier<SchemaBuilder> schemaBuilder =
        () ->
            SchemaBuilder.struct()
                .name("Kafka")
                .field("bigint", Schema.INT64_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA);

    return Stream.of(
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT64_SCHEMA).build(), 1_000_000L, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT32_SCHEMA).build(), 1_000_000, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT16_SCHEMA).build(),
            (short) 1_000_000,
            (short) 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.FLOAT32_SCHEMA).build(), 1_000_000F, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.FLOAT64_SCHEMA).build(), 1_000_000D, 1_000),
        Arguments.of(schemaBuilder.get().field("ttlcol", Schema.INT32_SCHEMA).build(), -1_000, 0));
  }

  private static Stream<? extends Arguments> timestampColProvider() {
    Supplier<SchemaBuilder> schemaBuilder =
        () ->
            SchemaBuilder.struct()
                .name("Kafka")
                .field("bigint", Schema.INT64_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA);

    return Stream.of(
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT64_SCHEMA).build(),
            1000L,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT32_SCHEMA).build(),
            1000,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT16_SCHEMA).build(),
            (short) 1000,
            (short) 1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.FLOAT32_SCHEMA).build(),
            1000F,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.FLOAT64_SCHEMA).build(),
            1000D,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT32_SCHEMA).build(),
            -1000,
            -1_000_000L));
  }

  private Map<String, String> makeConnectorProperties(String mappingString) {
    return makeConnectorProperties(mappingString, Collections.emptyMap());
  }

  private void assertTtl(int ttlValue, Number expectedTtlValue) {
    if (expectedTtlValue.equals(0)) {
      assertThat(ttlValue).isEqualTo(expectedTtlValue.intValue());
    } else {
      // actual ttl value can be less that or equal to expectedTtlValue because some time may elapse
      // between the moment the record was inserted and retrieved from db.
      assertThat(ttlValue).isLessThanOrEqualTo(expectedTtlValue.intValue()).isGreaterThan(0);
    }
  }
}
