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

import com.datastax.oss.common.sink.record.StructDataMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class StructDataMetadataTest {
  private final Schema schema;
  private Record<GenericRecord> record;
  private GenericRecordImpl struct;

  {
    RecordSchemaBuilder builder = SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("boolean").type(SchemaType.BOOLEAN);
    builder.field("double").type(SchemaType.DOUBLE);
    builder.field("float").type(SchemaType.FLOAT);
    builder.field("int").type(SchemaType.INT32);
    builder.field("text").type(SchemaType.STRING);
    builder.field("blob").type(SchemaType.BYTES);
    schema = Schema.generic(builder.build(SchemaType.AVRO));

    struct =
        new GenericRecordImpl()
            .put("bigint", 1L)
            .put("boolean", false)
            .put("double", 1d)
            .put("float", 1f)
            .put("int", 1)
            .put("text", "tt")
            .put("blob", new byte[10]);
    record = new PulsarRecordImpl(null, null, struct, schema);
  }

  private final LocalSchemaRegistry registry = new LocalSchemaRegistry();
  private final StructDataMetadata metadata =
      new StructDataMetadata(registry.ensureAndUpdateSchema(record));

  @Test
  void should_translate_field_types() {
    assertThat(getFieldType("bigint")).isEqualTo(GenericType.LONG);
    assertThat(getFieldType("boolean")).isEqualTo(GenericType.BOOLEAN);
    assertThat(getFieldType("double")).isEqualTo(GenericType.DOUBLE);
    assertThat(getFieldType("float")).isEqualTo(GenericType.FLOAT);
    assertThat(getFieldType("int")).isEqualTo(GenericType.INTEGER);
    assertThat(getFieldType("text")).isEqualTo(GenericType.STRING);
    assertThat(getFieldType("blob")).isEqualTo(GenericType.BYTE_BUFFER);
  }

  private GenericType<?> getFieldType(@NonNull String field) {
    return metadata.getFieldType(field, DataTypes.TEXT);
  }
}
