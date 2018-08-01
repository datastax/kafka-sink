/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class StructDataMetadataTest {
  private final Schema schema =
      SchemaBuilder.struct()
          .name("Kafka")
          .field("bigint", Schema.INT64_SCHEMA)
          .field("boolean", Schema.BOOLEAN_SCHEMA)
          .field("double", Schema.FLOAT64_SCHEMA)
          .field("float", Schema.FLOAT32_SCHEMA)
          .field("int", Schema.INT32_SCHEMA)
          .field("smallint", Schema.INT16_SCHEMA)
          .field("text", Schema.STRING_SCHEMA)
          .field("tinyint", Schema.INT8_SCHEMA)
          .field("blob", Schema.BYTES_SCHEMA)
          .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
          .field(
              "mapnested",
              SchemaBuilder.map(
                      Schema.STRING_SCHEMA,
                      SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                  .build())
          .field("list", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
          .field(
              "listnested",
              SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
          .build();
  private final StructDataMetadata metadata = new StructDataMetadata(schema);

  @Test
  void should_translate_field_types() {
    assertThat(getFieldType("bigint")).isEqualTo(GenericType.LONG);
    assertThat(getFieldType("boolean")).isEqualTo(GenericType.BOOLEAN);
    assertThat(getFieldType("double")).isEqualTo(GenericType.DOUBLE);
    assertThat(getFieldType("float")).isEqualTo(GenericType.FLOAT);
    assertThat(getFieldType("int")).isEqualTo(GenericType.INTEGER);
    assertThat(getFieldType("smallint")).isEqualTo(GenericType.SHORT);
    assertThat(getFieldType("text")).isEqualTo(GenericType.STRING);
    assertThat(getFieldType("tinyint")).isEqualTo(GenericType.BYTE);
    assertThat(getFieldType("blob")).isEqualTo(GenericType.BYTE_BUFFER);
    assertThat(getFieldType("map"))
        .isEqualTo(GenericType.mapOf(GenericType.STRING, GenericType.INTEGER));
    assertThat(getFieldType("mapnested"))
        .isEqualTo(
            GenericType.mapOf(
                GenericType.STRING, GenericType.mapOf(GenericType.INTEGER, GenericType.STRING)));
    assertThat(getFieldType("list")).isEqualTo(GenericType.listOf(GenericType.INTEGER));
    assertThat(getFieldType("listnested"))
        .isEqualTo(GenericType.listOf(GenericType.listOf(GenericType.INTEGER)));
  }

  private GenericType<?> getFieldType(@NotNull String field) {
    return metadata.getFieldType(field, DataTypes.TEXT);
  }
}
