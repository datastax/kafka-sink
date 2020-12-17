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
package com.datastax.oss.kafka.sink.record;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.record.RawData;
import com.datastax.oss.common.sink.record.StructData;
import com.datastax.oss.kafka.sink.KafkaStruct;
import java.nio.ByteBuffer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class StructDataTest {
  private final Schema schema =
      SchemaBuilder.struct()
          .name("Kafka")
          .field("bigint", Schema.INT64_SCHEMA)
          .field("boolean", Schema.BOOLEAN_SCHEMA)
          .field("bytes", Schema.BYTES_SCHEMA)
          .build();
  private final byte[] bytesArray = {3, 2, 1};
  private final Struct struct =
      new Struct(schema).put("bigint", 1234L).put("boolean", false).put("bytes", bytesArray);
  private final StructData structData = new StructData(new KafkaStruct(struct));

  @Test
  void should_parse_field_names_from_struct() {
    assertThat(structData.fields())
        .containsExactlyInAnyOrder(RawData.FIELD_NAME, "bigint", "boolean", "bytes");
  }

  @Test
  void should_get_field_value() {
    assertThat(structData.getFieldValue("bigint")).isEqualTo(1234L);
    assertThat(structData.getFieldValue("boolean")).isEqualTo(false);

    // Even though the record has a byte[], we must get a ByteBuffer when we
    // retrieve it because the driver requires the input to be a ByteBuffer
    // when encoding for a blob column.
    assertThat(structData.getFieldValue("bytes")).isEqualTo(ByteBuffer.wrap(bytesArray));
  }

  @Test
  void should_handle_null_struct() {
    StructData empty = new StructData(null);
    assertThat(empty.fields()).containsOnly(RawData.FIELD_NAME);
    assertThat(empty.getFieldValue("junk")).isNull();
  }
}
