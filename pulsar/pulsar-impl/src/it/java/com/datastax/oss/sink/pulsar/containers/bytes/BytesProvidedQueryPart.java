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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.sink.util.Tuple2;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
// minimum version required because support of non frozen types
public class BytesProvidedQueryPart extends BytesPart {
  private static final Schema UDT_SCHEMA =
      SchemaBuilder.record("pulsar")
          .fields()
          .optionalInt("udtmem1")
          .optionalString("udtmem2")
          .endRecord();

  @Override
  protected String basicName() {
    return "bytes-pq";
  }

  @BeforeEach
  void cleanup() {
    cassandraSession.execute("truncate types_with_frozen");
    cassandraSession.execute("truncate types");
  }

  @SuppressWarnings("unchecked")
  private void regSinkWithQuery(
      String name, String table, String mapping, String query, Tuple2<String, Object>... ext)
      throws PulsarAdminException {
    List<Tuple2<String, Object>> exts = new ArrayList<>();
    exts.add(Tuple2.of("topic." + name + ".testks." + table + ".query", query));
    exts.add(Tuple2.of("topic." + name + ".testks." + table + ".deletesEnabled", "false"));
    exts.addAll(Arrays.asList(ext));
    regSink(name, table, mapping, exts.toArray(new Tuple2[0]));
  }

  private void regSinkWithQuery(
      String name, String mapping, String query, Tuple2<String, Object>... ext)
      throws PulsarAdminException {
    regSinkWithQuery(name, "types", mapping, query, ext);
  }

  @Test
  void should_insert_json_using_query_parameter()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sijuqp");
    regSinkWithQuery(
        name,
        "bigintcol=value.bigint, intcol=value.int",
        "INSERT INTO testks.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)");

    String value = "{\"bigint\": 1234, \"int\": 10000}";

    Long recordTimestamp = 123456L;
    send(name, null, value.getBytes(), recordTimestamp);
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, intcol, writetime(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);

    // timestamp from record is ignored with user provided queries
    assertThat(row.getLong(2)).isGreaterThan(recordTimestamp);
  }

  @Test
  void
      should_allow_insert_json_using_query_parameter_with_bound_variables_different_than_cql_columns()
          throws PulsarAdminException, PulsarClientException {
    String name = name("saijuqpwbvdtcc");
    // when providing custom query, the connector is not validating bound variables from prepared
    // statements user needs to take care of the query requirements on their own.
    regSinkWithQuery(
        name,
        "some_name=value.bigint, some_name_2=value.int",
        "INSERT INTO testks.types (bigintCol, intCol) VALUES (:some_name, :some_name_2)");

    String value = "{\"bigint\": 1234, \"int\": 10000}";

    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
  }

  @Test
  void should_update_json_using_query_parameter()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sujuqp");
    regSinkWithQuery(
        name,
        "pkey=value.pkey, newitem=value.newitem",
        "UPDATE testks.types SET listCol = listCol + [1] where bigintcol = :pkey");

    String value = "{\"pkey\": 1234, \"newitem\": 1}";

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();
    producer.newMessage().value(value.getBytes()).send();
    producer.newMessage().value(value.getBytes()).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 2);
    producer.close();
    unregisterSink(name);

    // Verify that two values were append to listcol
    List<Row> results =
        cassandraSession.execute("SELECT * FROM types where bigintcol = 1234").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(ImmutableList.of(1, 1));
  }

  @Test
  void should_insert_json_using_query_parameter_and_ttl()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sijuqpattl");
    regSinkWithQuery(
        name,
        "bigintcol=value.bigint, intcol=value.int, ttl=value.ttl",
        "INSERT INTO testks.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TTL :ttl",
        Tuple2.of("topic." + name + ".testks.types.ttlTimeUnit", "HOURS"));

    String value = "{\"bigint\": 1234, \"int\": 10000, \"ttl\": 100000}";

    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, intcol, ttl(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
    assertTtl(row.getInt(2), 100000);
  }

  @Test
  void should_insert_json_using_query_parameter_and_timestamp()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sijuqpats");
    regSinkWithQuery(
        name,
        "bigintcol=value.bigint, intcol=value.int, timestamp=value.timestamp",
        "INSERT INTO testks.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) "
            + "USING TIMESTAMP :timestamp",
        Tuple2.of("topic." + name + ".testks.types.timestampTimeUnit", "HOURS"));

    String value = "{\"bigint\": 1234, \"int\": 10000, \"timestamp\": 100000}";

    send(name, null, value.getBytes());
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, intcol, writetime(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
    assertThat(row.getLong(2)).isEqualTo(100000L);
  }

  @Test
  void should_insert_struct_with_query_parameter()
      throws PulsarAdminException, PulsarClientException {
    String name = name("siswqp");
    regSinkWithQuery(
        name,
        "bigint_col=value.bigint, int_col=value.int, timestamp=value.int",
        "INSERT INTO testks.types (bigintCol, intCol) VALUES (:bigint_col, :int_col) "
            + "USING TIMESTAMP :timestamp and TTL 1000");
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredInt("int")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("int", 1000);

    send(name, null, wornBytes(value), 153000987L);
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession
            .execute("SELECT bigintcol, intcol, writetime(intcol), ttl(intcol) FROM types")
            .all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getInt("intcol")).isEqualTo(1000);
    assertThat(row.getLong(2)).isEqualTo(1000L);
    assertTtl(row.getInt(3), 1000);
  }

  @Test
  void should_use_query_to_partially_update_non_frozen_udt_when_null_to_unset()
      throws PulsarAdminException, PulsarClientException {
    String name = name("suqtpunfuwntu");
    regSinkWithQuery(
        name,
        "types_with_frozen",
        "bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2",
        "UPDATE testks.types_with_frozen set udtColNotFrozen.udtmem1=:udtcol1, "
            + "udtColNotFrozen.udtmem2=:udtcol2 where bigintCol=:bigintcol",
        Tuple2.of("topic." + name + ".testks.types_with_frozen.nullToUnset", "true"));

    GenericRecord value = new GenericData.Record(UDT_SCHEMA);
    value.put("udtmem1", 42);
    value.put("udtmem2", "the answer");

    send(name, "98761234", wornBytes(value));

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    Row row = extractAndAssertThatOneRowInResult(results);

    UDTValue udt = row.getUDTValue("udtColNotFrozen");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isEqualTo("the answer");

    // insert record with only one column from udt - udtmem2 is null
    value = new GenericData.Record(UDT_SCHEMA);
    value.put("udtmem1", 42);

    send(name, "98761234", wornBytes(value));

    results =
        cassandraSession.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    row = extractAndAssertThatOneRowInResult(results);

    // default for topic is nullToUnset, so the udtmem2 field was not updated, the value was not
    // overridden
    udt = row.getUDTValue("udtColNotFrozen");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isEqualTo("the answer");
  }

  @Test
  void should_use_update_query_on_non_frozen_udt_and_override_with_null_when_null_to_unset_false()
      throws PulsarAdminException, PulsarClientException {
    String name = name("suuqonfuaownwntuf");
    regSinkWithQuery(
        name,
        "types_with_frozen",
        "bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2",
        "UPDATE testks.types_with_frozen set udtColNotFrozen.udtmem1=:udtcol1, "
            + "udtColNotFrozen.udtmem2=:udtcol2 where bigintCol=:bigintcol",
        Tuple2.of("topic." + name + ".testks.types_with_frozen.nullToUnset", "false"));

    GenericRecord value = new GenericData.Record(UDT_SCHEMA);
    value.put("udtmem1", 42);
    value.put("udtmem2", "the answer");

    send(name, "98761234", wornBytes(value));

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    Row row = extractAndAssertThatOneRowInResult(results);

    UDTValue udt = row.getUDTValue("udtColNotFrozen");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isEqualTo("the answer");
    // insert record with only one column from udt - udtmem2 is null
    value = new GenericData.Record(UDT_SCHEMA);
    value.put("udtmem1", 42);

    send(name, "98761234", wornBytes(value));

    unregisterSink(name);

    results =
        cassandraSession.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    row = extractAndAssertThatOneRowInResult(results);

    // nullToUnset for this topic was set to false, so the udtmem2 field was updated, the value was
    // overridden with null
    udt = row.getUDTValue("udtColNotFrozen");
    assertThat(udt.getInt("udtmem1")).isEqualTo(42);
    assertThat(udt.getString("udtmem2")).isNull();
  }

  @Test
  void should_use_query_to_partially_update_map_when_null_to_unset()
      throws PulsarAdminException, PulsarClientException {
    String name = name("suqtpumwnou");
    regSinkWithQuery(
        name,
        "pk=value.pk, key=value.key, value=value.value",
        "UPDATE testks.types SET mapCol[:key]=:value where bigintcol = :pk",
        Tuple2.of("topic." + name + ".testks.types.nullToUnset", "true"));

    String value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": 10}}";
    send(name, null, value.getBytes());

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    Row row = extractAndAssertThatOneRowInResult(results);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(1);
    assertThat(mapcol).containsEntry("key_1", 10);

    value = "{\"pk\": 42, \"key\": \"key_1\", \"value\": null}";
    send(name, null, value.getBytes());
    unregisterSink(name);
    results = cassandraSession.execute("SELECT * FROM types").all();
    row = extractAndAssertThatOneRowInResult(results);
    mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(1);
    // update will null value will be skipped because nullToUnset = true
    assertThat(mapcol).containsEntry("key_1", 10);
  }

  @Test
  void should_use_query_to_partially_update_map_and_remove_when_using_null_to_unset_false()
      throws PulsarAdminException, PulsarClientException {
    String name = name("suqtpumarwuntuf");
    regSinkWithQuery(
        name,
        "pk=value.pk, key=value.key, value=value.value",
        "UPDATE testks.types SET mapCol[:key]=:value where bigintcol = :pk",
        Tuple2.of("topic." + name + ".testks.types.nullToUnset", "false"));

    String value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": 10}}";
    send(name, null, value.getBytes());

    // Verify that the record was inserted properly in the database.
    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    Row row = extractAndAssertThatOneRowInResult(results);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assertThat(mapcol).isNotNull();
    assertThat(mapcol.size()).isEqualTo(1);
    assertThat(mapcol).containsEntry("key_1", 10);

    value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": null}";
    send(name, null, value.getBytes());
    results = cassandraSession.execute("SELECT * FROM types").all();
    // setting value for map = null when nullToUnset = false will cause the record to be removed
    assertThat(results.size()).isEqualTo(0);
  }

  @NonNull
  private Row extractAndAssertThatOneRowInResult(List<Row> results) {
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    return row;
  }
}
