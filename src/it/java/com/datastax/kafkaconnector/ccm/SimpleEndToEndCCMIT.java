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
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
class SimpleEndToEndCCMIT extends EndToEndCCMITBase {
  private DseSinkConnector conn = new DseSinkConnector();
  private DseSinkTask task = new DseSinkTask();

  public SimpleEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @BeforeAll
  void createTables() {
    session.execute(
        "CREATE TABLE IF NOT EXISTS types ("
            + "bigintCol bigint PRIMARY KEY, "
            + "booleanCol boolean, "
            + "doubleCol double, "
            + "floatCol float, "
            + "intCol int, "
            + "smallintCol smallint, "
            + "textCol text, "
            + "tinyIntCol tinyint, "
            + "mapCol map<text, int>, "
            + "mapNestedCol frozen<map<text, map<int, text>>>, "
            + "listCol list<int>, "
            + "setCol set<int>, "
            + "tupleCol tuple<int, int, int>"
            + ")");
  }

  @BeforeEach
  void truncateTable() {
    session.execute("TRUNCATE types");
  }

  @AfterEach
  void stopConnector() {
    task.stop();
    conn.stop();
  }

  @Test
  void struct_value_only() {
    Map<String, String> props =
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
                + "setcol=value.set, "
                + "tuplecol=value.tuple"
        );

    conn.start(props);

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
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
            .field("mapnested", SchemaBuilder.map(Schema.STRING_SCHEMA,
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA)))
            .field("list", SchemaBuilder.array(Schema.INT32_SCHEMA))
            .field("set", SchemaBuilder.array(Schema.INT32_SCHEMA))
            .field("tuple", SchemaBuilder.array(Schema.INT32_SCHEMA));

    Map<String, Integer> mapValue = ImmutableMap.<String, Integer>builder()
        .put("sub1", 37)
        .put("sub2", 96)
        .build();

    Map<String, Map<Integer, String>> nestedMapValue = ImmutableMap.<String, Map<Integer, String>>builder()
        .put("sub1", ImmutableMap.<Integer, String>builder()
            .put(37, "sub1sub1")
            .put(96, "sub1sub2")
            .build())
        .put("sub2", ImmutableMap.<Integer, String>builder()
            .put(47, "sub2sub1")
            .put(90, "sub2sub2")
            .build())
        .build();

    List<Integer> listValue = Arrays.asList(37, 96, 90);

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
            .put("set", listValue)
            .put("tuple", listValue);

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    task.start(props);
    task.put(Collections.singletonList(record));

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
    assertThat(row.getList("setcol", Integer.class)).isEqualTo(listValue);
    assertThat(row.getList("tuplecol", Integer.class)).isEqualTo(listValue);
  }

  @Test
  void simple_json_value_only() {
    Map<String, String> props =
        makeConnectorProperties(
            "bigintcol=value.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=value.double, "
                + "floatcol=value.float, "
                + "intcol=value.int, "
                + "smallintcol=value.smallint, "
                + "textcol=value.text, "
                + "tinyintcol=value.tinyint");

    conn.start(props);

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
    task.start(props);
    task.put(Collections.singletonList(record));

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
    Map<String, String> props = makeConnectorProperties("bigintcol=value.f1, mapcol=value.f2");
    conn.start(props);
    String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    task.start(props);
    task.put(Collections.singletonList(record));

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
    Map<String, String> props =
        makeConnectorProperties(
            "bigintcol=key.bigint, "
                + "booleancol=value.boolean, "
                + "doublecol=key.double, "
                + "floatcol=value.float, "
                + "intcol=key.int, "
                + "smallintcol=value.smallint, "
                + "textcol=key.text, "
                + "tinyintcol=value.tinyint");
    conn.start(props);

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
            .field("tinyint", Schema.INT8_SCHEMA);
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
    task.start(props);
    task.put(Collections.singletonList(record));

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

  private Map<String, String> makeConnectorProperties(String mappingString) {
    return ImmutableMap.<String, String>builder()
        .put(
            "contactPoints",
            ccm.getInitialContactPoints()
                .stream()
                .map(addr -> String.format("%s", addr.getHostAddress()))
                .collect(Collectors.joining(",")))
        .put("port", String.format("%d", ccm.getBinaryPort()))
        .put("loadBalancing.localDc", "Cassandra")
        .put("mapping", mappingString)
        .put("keyspace", session.getKeyspace().get().asCql(true))
        .put("table", "types")
        .build();
  }
}
