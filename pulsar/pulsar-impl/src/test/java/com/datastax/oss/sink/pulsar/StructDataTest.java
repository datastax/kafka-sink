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
package com.datastax.oss.sink.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.record.RawData;
import com.datastax.oss.common.sink.record.StructData;
import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class StructDataTest {

  static class Model {

    long bigint;
    boolean bool;
    byte[] bytes;

    public long getBigint() {
      return bigint;
    }

    public void setBigint(long bigint) {
      this.bigint = bigint;
    }

    public boolean isBool() {
      return bool;
    }

    public void setBool(boolean bool) {
      this.bool = bool;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }
  }

  private final Schema schema = Schema.AVRO(Model.class);
  private final byte[] bytesArray = {3, 2, 1};
  private final GenericRecordImpl struct =
      new GenericRecordImpl().put("bigint", 1234L).put("bool", false).put("bytes", bytesArray);
  private final Record<GenericRecord> record = new PulsarRecordImpl(null, null, struct, schema);
  private final StructData structData =
      new StructData(PulsarStruct.ofRecord(record, new LocalSchemaRegistry()));

  @Test
  void should_parse_field_names_from_struct() {
    assertThat(structData.fields())
        .containsExactlyInAnyOrder(RawData.FIELD_NAME, "bigint", "bool", "bytes");
  }

  @Test
  void should_get_field_value() {
    assertThat(structData.getFieldValue("bigint")).isEqualTo(1234L);
    assertThat(structData.getFieldValue("bool")).isEqualTo(false);

    // Even though the record has a byte[], we must get a ByteBuffer when we
    // retrieve it because the driver requires the input to be a ByteBuffer
    // when encoding for a blob column.
    assertThat(structData.getFieldValue("bytes")).isEqualTo(ByteBuffer.wrap(bytesArray));
  }
}
