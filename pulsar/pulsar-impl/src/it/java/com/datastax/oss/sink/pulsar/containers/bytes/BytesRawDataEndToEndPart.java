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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.util.Tuple2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("container")
public class BytesRawDataEndToEndPart extends BytesPart {

  @Override
  protected String basicName() {
    return "bytes-raw";
  }

  @AfterEach
  void teardown() {
    cassandraSession.execute("truncate types");
  }

  @Test
  void raw_bigint_value() throws PulsarAdminException, PulsarClientException {
    String name = name("rbv");
    regSink(name, "types", "bigintcol=value");

    send(name, null, longBytes(5725368L));
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void should_insert_from_topic_with_complex_name()
      throws PulsarAdminException, PulsarClientException {
    String name = "this.is.complex_topic-name";
    regSink(name, "types", "bigintcol=value");
    //    initConnectorAndTask(
    //        makeConnectorProperties("bigintcol=value", "types", null,
    // "this.is.complex_topic-name"));

    send(name, null, longBytes(5725368L));
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_snappy() throws PulsarAdminException, PulsarClientException {
    String name = name("twvs");
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    regSink(name, "types", "bigintcol=value", Tuple2.of("compression", "Snappy"));

    send(name, null, longBytes(5725368L));
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_lz4() throws PulsarAdminException, PulsarClientException {
    // Technically, this doesn't test compression because it's possible that the connector
    // ignores the setting entirely and just issues requests as usual. A more strict test
    // would gather metrics on bytes sent during the test and make sure it's less than
    // the number of bytes sent when run without compression. In any case, if this were
    // to ever break, it's more likely it will fail non-silently.
    String name = name("rbvlz");
    regSink(name, "types", "bigintcol=value", Tuple2.of("compression", "LZ4"));

    send(name, null, longBytes(5725368L));

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_string_value() throws PulsarAdminException, PulsarClientException {
    String name = "rsv";
    regSink(name, "types", "bigintcol=key, textcol=value");

    send(name, String.valueOf(98761234L), "my text".getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getString("textcol")).isEqualTo("my text");
  }

  @Test
  void raw_byte_array_value() throws PulsarAdminException, PulsarClientException {
    String name = name("rbav");
    regSink(name, "types", "bigintcol=key, blobcol=value");

    byte[] bytes = new byte[] {1, 2, 3};
    send(name, String.valueOf(98761234L), bytes);
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, blobcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    ByteBuffer blobcol = row.getBytes("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(bytes);
  }

  // only records are supported yet
  /*
  @Test
  void raw_list_value_from_json()  {
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
  void raw_list_value_from_list()  {
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
  void null_to_unset_true() throws PulsarAdminException, PulsarClientException {
    String name = name("ntut");
    // Make a row with some value for textcol to start with.
    cassandraSession.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    regSink(name, "types", "bigintcol=key, textcol=value");

    Record<byte[]> record = mockRecord("mytopic", "1234567", null, 1234);
    send(name, "1234567", null);
    unregisterSink(name);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isEqualTo("got here");
  }

  @Test
  void null_to_unset_false() throws PulsarAdminException, PulsarClientException {
    String name = name("ntuf");
    // Make a row with some value for textcol to start with.
    cassandraSession.execute("INSERT INTO types (bigintcol, textcol) VALUES (1234567, 'got here')");

    regSink(
        name,
        "types",
        "bigintcol=key, textcol=value",
        Tuple2.of("topic." + name + ".testks.types.nullToUnset", "false"));

    send(name, "1234567", null);
    unregisterSink(name);

    // Verify that the record was inserted properly in the database; textcol should be unchanged.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, textcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getString("textcol")).isNull();
  }

  @Test
  void map_only() throws JsonProcessingException, PulsarAdminException, PulsarClientException {
    // given
    String name = name("maponly");
    String mapping =
        "bigintcol=value.bigint, "
            + "booleancol=value.boolean, "
            + "doublecol=value.double, "
            + "floatcol=value.float, "
            + "intcol=value.int, "
            + "smallintcol=value.smallint, "
            + "textcol=value.text,"
            + "mapnestedcol=value.mapnested,"
            + "mapcol=value.map,"
            + "tinyintcol=value.tinyint";
    regSink(name, "types", mapping);

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
    send(name, null, jsonval.getBytes());
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
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getObject("mapnestedcol")).isEqualTo(nestedMapValue);
  }

  @Test
  void raw_udt_value_map()
      throws JsonProcessingException, PulsarAdminException, PulsarClientException {
    // given
    String name = name("ruvm");
    regSink(name, "types", "bigintcol=key, listudtcol=value");

    Map<String, Object> value = new HashMap<>();
    value.put("a", 42);
    value.put("b", "the answer");
    value.put("c", Arrays.asList(1, 2, 3));

    String jsonval = new ObjectMapper().writeValueAsString(value);
    send(name, String.valueOf(98761234L), jsonval.getBytes());
    unregisterSink(name);

    // then
    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT bigintcol, listudtcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    UDTValue udt = row.getUDTValue("listudtcol");
    assertThat(udt.getInt("a")).isEqualTo(42);
    assertThat(udt.getString("b")).isEqualTo("the answer");
    assertThat(udt.getList("c", Integer.class)).isEqualTo(Arrays.asList(1, 2, 3));
  }

  /** Test for KAF-84. */
  @Test
  void raw_udt_value_map_case_sensitive()
      throws JsonProcessingException, PulsarAdminException, PulsarClientException {
    String name = name("ruvmcs");
    // given
    cassandraSession.execute(
        "CREATE TYPE case_sensitive_udt (\"Field A\" int, \"Field-B\" text, \"Field.C\" list<int>)");

    cassandraSession.execute(
        "CREATE TABLE \"CASE_SENSITIVE_UDT\" (pk bigint PRIMARY KEY, value frozen<case_sensitive_udt>)");

    regSink(name, "\"CASE_SENSITIVE_UDT\"", "pk=key, value=value");

    Map<String, Object> value = new HashMap<>();
    value.put("Field A", 42);
    value.put("Field-B", "the answer");
    value.put("Field.C", Arrays.asList(1, 2, 3));

    String jsonval = new ObjectMapper().writeValueAsString(value);
    send(name, String.valueOf(98761234L), jsonval.getBytes());
    unregisterSink(name);

    // then
    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT pk, value FROM \"CASE_SENSITIVE_UDT\"").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("pk")).isEqualTo(98761234L);
    UDTValue udtValue = row.getUDTValue("value");
    assertThat(udtValue).isNotNull();
    assertThat(udtValue.getInt("\"Field A\"")).isEqualTo(42);
    assertThat(udtValue.getString("\"Field-B\"")).isEqualTo("the answer");
    assertThat(udtValue.getList("\"Field.C\"", Integer.class)).containsExactly(1, 2, 3);
  }
}
