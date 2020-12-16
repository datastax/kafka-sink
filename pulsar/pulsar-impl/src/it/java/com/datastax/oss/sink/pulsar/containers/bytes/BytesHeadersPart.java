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

import static org.assertj.core.api.Assertions.*;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.util.StringUtil;
import com.datastax.oss.sink.util.Tuple2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("containers")
public class BytesHeadersPart extends BytesPart {

  @Override
  protected String basicName() {
    return "bytes-headers";
  }

  @AfterEach
  void teardown() {
    cassandraSession.execute("truncate pk_value");
    cassandraSession.execute("truncate types");
  }

  /** Test for KAF-142 */
  @Test
  void should_use_header_values_as_ttl_and_timestamp()
      throws PulsarAdminException, PulsarClientException {
    String name = name("suhvatat");
    regSink(
        name,
        "types",
        "bigintcol=header.bigint, doublecol=header.double, __ttl=header.ttlcolumn, __timestamp = header.timestampcolumn",
        Tuple2.of("topic." + name + ".testks.types.timestampTimeUnit", "MILLISECONDS"));

    long ttlValue = 1_000_000;
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("bigint", "100000")
            .put("ttlcolumn", String.valueOf(ttlValue))
            .put("timestampcolumn", "2345678")
            .put("double", "100.0")
            .build();

    send(name, null, null, props);

    List<Row> results =
        cassandraSession
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
  void should_delete_when_header_values_are_null()
      throws PulsarAdminException, PulsarClientException {
    String name = name("sdwhvan");
    // First insert a row...
    cassandraSession.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(name, "pk_value", "my_pk=header.my_pk, my_value=header.my_value");

    Map<String, String> headers =
        new HashMap<String, String>() {
          {
            put("my_pk", "1234567");
            put("my_value>null", "");
          }
        };

    send(name, null, null, headers);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  /** Test for KAF-142 */
  @Test
  void should_use_values_from_header_in_mapping()
      throws JsonProcessingException, PulsarAdminException, PulsarClientException {
    String name = name("suvfhim");
    // values used in this test are random and irrelevant for the test
    // given
    String mapping =
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
            + "booleanudtcol=header.booleanudt";
    regSink(name, "types", mapping);

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
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> headers =
        new HashMap<String, String>() {
          {
            put("bigint", baseValue.toString());
            put("double", String.valueOf(baseValue.doubleValue()));
            put("text", "value");
            put("boolean", "false");
            put("tinyint", String.valueOf(baseValue.byteValue()));
            put("blob", StringUtil.bytesToString(blobValue));
            put("float", String.valueOf(baseValue.floatValue()));
            put("int", String.valueOf(baseValue.intValue()));
            put("smallint", String.valueOf(baseValue.shortValue()));
            put("map", mapper.writeValueAsString(mapValue));
            put("mapnested", mapper.writeValueAsString(nestedMapValue));
            put("list", mapper.writeValueAsString(listValue));
            put("listnested", mapper.writeValueAsString(nestedListValue));
            put("set", mapper.writeValueAsString(listValue));
            put("setnested", mapper.writeValueAsString(nestedListValue));
            put("booleanudt", mapper.writeValueAsString(booleanUdtValue));
          }
        };

    String json = "{\"bigint\": 1234567}";
    send(name, null, json.getBytes(), headers);

    List<Row> results = cassandraSession.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue.longValue());
    assertThat(row.getDouble("doublecol")).isEqualTo(baseValue.doubleValue());
    assertThat(row.getString("textcol")).isEqualTo("value");
    assertThat(row.getBool("booleancol")).isEqualTo(false);
    assertThat(row.getByte("tinyintcol")).isEqualTo(baseValue.byteValue());
    ByteBuffer blobcol = row.getBytes("blobcol");
    assertThat(blobcol).isNotNull();
    assertThat(Bytes.getArray(blobcol)).isEqualTo(blobValue);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue());
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
    assertThat(row.getShort("smallintcol")).isEqualTo(baseValue.shortValue());
    assertThat(row.getMap("mapcol", String.class, Integer.class)).isEqualTo(mapValue);
    assertThat(row.getObject("mapnestedcol")).isEqualTo(nestedMapValue);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(listValue);
    assertThat(row.getObject("listnestedcol"))
        .isEqualTo(
            new ArrayList<Set>(Arrays.asList(new HashSet<>(listValue), new HashSet<>(list2))));
    assertThat(row.getSet("setcol", Integer.class)).isEqualTo(new HashSet<>(listValue));
    assertThat(row.getObject("setnestedcol")).isEqualTo(new HashSet<>(nestedListValue));
    UDTValue udt = row.getUDTValue("booleanudtcol");
    assertThat(udt.getBool("udtmem1")).isTrue();
    assertThat(udt.getString("udtmem2")).isEqualTo("false");
  }
}
