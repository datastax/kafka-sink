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
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("containers")
public class BytesNowFunctionPart extends BytesPart {

  @Override
  protected String basicName() {
    return "bytes-now";
  }

  @AfterEach
  void teardown() {
    cassandraSession.execute("truncate types");
    cassandraSession.execute("truncate pk_value_with_timeuuid");
  }

  @Test
  void should_insert_value_using_now_function_json()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sivunfj");
    regSink(name, "types", "bigintcol=value.bigint, loaded_at=now()");

    // when
    String json = "{\"bigint\": 1234567}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // then
    List<Row> results = cassandraSession.execute("SELECT bigintcol, loaded_at FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getUUID("loaded_at")).isLessThanOrEqualTo(Uuids.timeBased());
  }

  @Test
  void should_insert_value_using_now_function_for_two_dse_columns()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sivunfftdc");
    regSink(name, "types", "bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()");

    // when
    String json = "{\"bigint\": 1234567}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // then
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.getUUID("loaded_at");
    UUID loadedAt2 = row.getUUID("loaded_at2");
    // columns inserted using now() should have different TIMEUUID values
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void should_insert_value_using_now_function_avro()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sivunfa");
    regSink(name, "types", "bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()");

    Schema schema = SchemaBuilder.record("pulsar").fields().requiredLong("bigint").endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);

    send(name, null, wornBytes(value));
    unregisterSink(name);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        cassandraSession.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.getUUID("loaded_at");
    UUID loadedAt2 = row.getUUID("loaded_at2");
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void delete_simple_key_json_when_using_now_function_in_mapping()
      throws PulsarAdminException, PulsarClientException {
    String name = name("dskjwunfim");
    // First insert a row...
    cassandraSession.execute(
        "INSERT INTO pk_value_with_timeuuid (my_pk, my_value, loaded_at) VALUES (1234567, true, now())");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(1);

    // now() function call is ignored when null value is send - DELETE will be performed
    regSink(
        name,
        "pk_value_with_timeuuid",
        "my_pk=value.my_pk, my_value=value.my_value, loaded_at=now()");

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
