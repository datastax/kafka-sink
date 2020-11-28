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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.sink.pulsar.AvroAPIAdapter;
import com.datastax.oss.sink.record.StructDataMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class StructDataMetadataTest {

  private final AvroAPIAdapter<?> adapter = new AvroAPIAdapter();

  private final Schema schema =
      SchemaBuilder.record("Pulsar")
          .fields()
          .optionalLong("bigint")
          .requiredBoolean("boolean")
          .requiredDouble("double")
          .requiredFloat("float")
          .requiredInt("int")
          .optionalString("text")
          .requiredInt("tinyint")
          .requiredInt("smallint")
          .requiredBytes("blob")
          .name("map")
          .type()
          .map()
          .values()
          .intType()
          .noDefault()
          .name("mapnested")
          .type(Schema.createMap(Schema.createMap(Schema.create(Schema.Type.DOUBLE))))
          .noDefault()
          .name("list")
          .type()
          .array()
          .items()
          .intType()
          .noDefault()
          .name("listnested")
          .type(Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT))))
          .noDefault()
          .endRecord();

  private final StructDataMetadata<Schema> metadata = new StructDataMetadata<>(schema, adapter);

  @Test
  void should_translate_field_types() {
    assertThat(getFieldType("bigint")).isEqualTo(GenericType.LONG);
    assertThat(getFieldType("boolean")).isEqualTo(GenericType.BOOLEAN);
    assertThat(getFieldType("double")).isEqualTo(GenericType.DOUBLE);
    assertThat(getFieldType("float")).isEqualTo(GenericType.FLOAT);
    assertThat(getFieldType("int")).isEqualTo(GenericType.INTEGER);
    assertThat(getFieldType("smallint")).isEqualTo(GenericType.INTEGER);
    assertThat(getFieldType("text")).isEqualTo(GenericType.STRING);
    assertThat(getFieldType("tinyint")).isEqualTo(GenericType.INTEGER);
    assertThat(getFieldType("blob")).isEqualTo(GenericType.BYTE_BUFFER);
    assertThat(getFieldType("map"))
        .isEqualTo(GenericType.mapOf(GenericType.STRING, GenericType.INTEGER));
    assertThat(getFieldType("mapnested"))
        .isEqualTo(
            GenericType.mapOf(
                GenericType.STRING, GenericType.mapOf(GenericType.STRING, GenericType.DOUBLE)));
    assertThat(getFieldType("list")).isEqualTo(GenericType.listOf(GenericType.INTEGER));
    assertThat(getFieldType("listnested"))
        .isEqualTo(GenericType.listOf(GenericType.listOf(GenericType.INTEGER)));
  }

  private GenericType<?> getFieldType(@NonNull String field) {
    return metadata.getFieldType(field, DataTypes.TEXT);
  }
}
