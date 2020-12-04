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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.sink.pulsar.GenSchemaGenericRecordSink;
import com.datastax.oss.sink.util.StringUtil;
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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.config.ConfigException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class GenHeadersCCMIT extends EndToEndCCMITBase<GenericRecord> {

  GenHeadersCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new GenSchemaGenericRecordSink());
  }

  /** Test for KAF-142 */
  @Test
  void should_use_header_values_as_ttl_and_timestamp() {
    Map<String, Object> cfg =
        makeConnectorProperties(
            "bigintcol=header.bigint, doublecol=header.double, __ttl=header.ttlcolumn, __timestamp = header.timestampcolumn",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS"));
    initConnectorAndTask(cfg);

    long ttlValue = 1_000_000;
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("bigint", "100000")
            .put("ttlcolumn", String.valueOf(ttlValue))
            .put("timestampcolumn", "2345678")
            .put("double", "100.0")
            .build();

    // when
    sendRecord(mockRecord("mytopic", null, null, 1234, 1L, props));

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

    initConnectorAndTask(
        makeConnectorProperties("my_pk=header.my_pk, my_value=header.my_value", "pk_value", null));

    Map<String, String> headers =
        new HashMap<String, String>() {
          {
            put("my_pk", "1234567");
            put("my_value", null);
          }
        };

    sendRecord(mockRecord("mytopic", null, null, 1234, 1L, headers));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  /** Test for KAF-142 */
  @Test
  void should_use_values_from_header_in_mapping() throws JsonProcessingException {
    // values used in this test are random and irrelevant for the test
    // given
    initConnectorAndTask(
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

    org.apache.avro.generic.GenericRecord rec =
        new GenericData.Record(
            SchemaBuilder.record("try").fields().requiredLong("bigint").endRecord());
    rec.put("bigint", 1234567l);

    // when
    sendRecord(mockRecord("mytopic", null, pulsarGenericAvroRecord(rec), 1234, 1L, headers));

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
    booleanUdt.attach(session.getContext());
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "false"));
  }

  @Test
  void should_fail_when_using_header_without_specific_field_in_a_mapping() {
    assertThatThrownBy(
            () -> initConnectorAndTask(makeConnectorProperties("bigintcol=key, udtcol=header")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid field name 'header': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.' or 'header.', or be one of supported functions: '[now()]'");
  }
}
