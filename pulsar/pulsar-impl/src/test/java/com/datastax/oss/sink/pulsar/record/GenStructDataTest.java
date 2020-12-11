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
package com.datastax.oss.sink.pulsar.record;

import static org.assertj.core.api.Assertions.*;

import com.datastax.oss.sink.pulsar.GenAPIAdapter;
import com.datastax.oss.sink.pulsar.TestUtil;
import com.datastax.oss.sink.pulsar.gen.GenSchema;
import com.datastax.oss.sink.pulsar.gen.GenStruct;
import com.datastax.oss.sink.record.RawData;
import com.datastax.oss.sink.record.StructData;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenStructDataTest {

  private final GenAPIAdapter adapter = new GenAPIAdapter();
  private StructData<GenStruct> structData;

  @BeforeEach
  void setup() {
    Schema schema =
        SchemaBuilder.record("Pulsar")
            .fields()
            .optionalLong("bigint")
            .optionalBoolean("boolean")
            .optionalBytes("bytes")
            .endRecord();
    GenericRecord struct = new GenericData.Record(schema);
    struct.put("bigint", 1234L);
    struct.put("boolean", false);
    struct.put("bytes", ByteBuffer.wrap(new byte[] {3, 2, 1}));
    GenStruct genValue = GenSchema.convert(TestUtil.pulsarGenericAvroRecord(struct));

    structData = new StructData<>(genValue, adapter);
  }

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
    assertThat(structData.getFieldValue("bytes")).isEqualTo(ByteBuffer.wrap(new byte[] {3, 2, 1}));
  }

  @Test
  void should_handle_null_struct() {
    StructData empty = new StructData<>(null, adapter);
    assertThat(empty.fields()).containsOnly(RawData.FIELD_NAME);
    assertThat(empty.getFieldValue("junk")).isNull();
  }
}
