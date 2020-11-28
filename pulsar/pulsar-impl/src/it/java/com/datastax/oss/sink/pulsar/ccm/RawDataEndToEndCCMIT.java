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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.pulsar.BytesSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class RawDataEndToEndCCMIT extends EndToEndCCMITBase<byte[]> {

  RawDataEndToEndCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new BytesSink(true));
  }

  @Test
  void raw_bigint_value() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=value"));

    Record<byte[]> record = mockRecord("mytopic", null, longBytes(5725368L), 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void should_insert_from_topic_with_complex_name() throws Exception {
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=value", "types", null, "this.is.complex_topic-name"));

    Record<byte[]> record =
        mockRecord("this.is.complex_topic-name", null, longBytes(5725368L), 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_snappy() throws Exception {
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=value", ImmutableMap.of("compression", "Snappy")));

    Record<byte[]> record = mockRecord("mytopic", null, longBytes(5725368L), 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_lz4() throws Exception {
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=value", ImmutableMap.of("compression", "LZ4")));

    Record<byte[]> record = mockRecord("mytopic", null, longBytes(5725368L), 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_string_value() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, textcol=value"));

    Record<byte[]> record =
        mockRecord("mytopic", String.valueOf(98761234L), "my text".getBytes(), 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getString("textcol")).isEqualTo("my text");
  }

  @Test
  void raw_byte_array_value() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, blobcol=value"));

    byte[] bytes = new byte[] {1, 2, 3};
    Record<byte[]> record = mockRecord("mytopic", String.valueOf(98761234L), bytes, 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, blobcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    ByteBuffer blobcol = row.getByteBuffer("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(bytes);
  }

  // only records are supported yet
  /*
  @Test
  void raw_list_value_from_json() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, listcol=value"));

    Record<byte[]> record = mockRecord("mytopic", String.valueOf(98761234L), "[42, 37]".getBytes(), 1234);

    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }

  @Test
  void raw_list_value_from_list() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, listcol=value"));

    Record<byte[]> record = mockRecord("mytopic", String.valueOf(98761234L), "[42, 37]".getBytes(), 1234);

    GenericArray value =
    SinkRecord record =
        new SinkRecord("mytopic", 0, null, 98761234L, null, Arrays.asList(42, 37), 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }
  */

  @Test
  void null_to_unset_true() throws Exception {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    initConnectorAndTask(makeConnectorProperties("bigintcol=key, textcol=value"));

    Record<byte[]> record = mockRecord("mytopic", "1234567", null, 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isEqualTo("got here");
  }

  @Test
  void null_to_unset_false() throws Exception {
    // Make a row with some value for textcol to start with.
    session.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=key, textcol=value",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.types.nullToUnset", keyspaceName), "false")));

    Record<byte[]> record = mockRecord("mytopic", "1234567", null, 1234);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = session.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isNull();
  }

  @Test
  void map_only() throws Exception {
    // given
    initConnectorAndTask(
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

    String jsonval = new ObjectMapper().writeValueAsString(value);
    Record<byte[]> record = mockRecord("mytopic", null, jsonval.getBytes(), 1234L);
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
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getMap("mapnestedcol", String.class, Map.class)).isEqualTo(nestedMapValue);
  }

  @Test
  void raw_udt_value_map() throws Exception {
    // given
    initConnectorAndTask(makeConnectorProperties("bigintcol=key, listudtcol=value"));

    Map<String, Object> value = new HashMap<>();
    value.put("a", 42);
    value.put("b", "the answer");
    value.put("c", Arrays.asList(1, 2, 3));

    String jsonval = new ObjectMapper().writeValueAsString(value);
    Record<byte[]> record =
        mockRecord("mytopic", String.valueOf(98761234L), jsonval.getBytes(), 1234L);

    // when
    runTaskWithRecords(record);

    // then
    // Verify that the record was inserted properly in the database.
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
    udt.attach(session.getContext());
    assertThat(row.getUdtValue("listudtcol"))
        .isEqualTo(udt.newValue(42, "the answer", Arrays.asList(1, 2, 3)));
  }

  /** Test for KAF-84. */
  @Test
  void raw_udt_value_map_case_sensitive() throws Exception {
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

    initConnectorAndTask(
        makeConnectorProperties("pk=key, value=value", "\"CASE_SENSITIVE_UDT\"", null));

    Map<String, Object> value = new HashMap<>();
    value.put("Field A", 42);
    value.put("Field-B", "the answer");
    value.put("Field.C", Arrays.asList(1, 2, 3));

    String jsonval = new ObjectMapper().writeValueAsString(value);
    Record<byte[]> record =
        mockRecord("mytopic", String.valueOf(98761234L), jsonval.getBytes(), 1234L);

    // when
    runTaskWithRecords(record);

    // then
    // Verify that the record was inserted properly in the database.
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
}
