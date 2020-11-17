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
package com.datastax.oss.sink.kafka.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class HeadersCCMIT extends EndToEndCCMITBase {

  HeadersCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
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

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
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
    booleanUdt.attach(session.getContext());
    assertThat(row.getUdtValue("booleanudtcol")).isEqualTo(booleanUdt.newValue(true, "false"));
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
}
