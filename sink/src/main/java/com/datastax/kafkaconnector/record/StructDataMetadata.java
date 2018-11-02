/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;

/** Metadata associated with a {@link StructData}. */
public class StructDataMetadata implements RecordMetadata {
  private static final ImmutableMap<Schema.Type, GenericType<?>> TYPE_MAP =
      ImmutableMap.<Schema.Type, GenericType<?>>builder()
          .put(Schema.Type.BOOLEAN, GenericType.BOOLEAN)
          .put(Schema.Type.FLOAT64, GenericType.DOUBLE)
          .put(Schema.Type.INT64, GenericType.LONG)
          .put(Schema.Type.FLOAT32, GenericType.FLOAT)
          .put(Schema.Type.INT8, GenericType.BYTE)
          .put(Schema.Type.INT16, GenericType.SHORT)
          .put(Schema.Type.INT32, GenericType.INTEGER)
          .put(Schema.Type.STRING, GenericType.STRING)
          .put(Schema.Type.BYTES, GenericType.BYTE_BUFFER)
          .build();
  private final Schema schema;

  public StructDataMetadata(@NotNull Schema schema) {
    this.schema = schema;
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    if (field.equals(RawData.FIELD_NAME)) {
      return GenericType.of(Struct.class);
    }
    Schema fieldType = schema.field(field).schema();
    return getGenericType(fieldType);
  }

  @NotNull
  private GenericType<?> getGenericType(@NotNull Schema fieldType) {
    GenericType<?> result = TYPE_MAP.get(fieldType.type());
    if (result != null) {
      return result;
    }
    // This is a complex type.
    // TODO: PERF: Consider caching these results and check the cache before creating
    // new entries.

    switch (fieldType.type()) {
      case ARRAY:
        return GenericType.listOf(getGenericType(fieldType.valueSchema()));
      case MAP:
        return GenericType.mapOf(
            getGenericType(fieldType.keySchema()), getGenericType(fieldType.valueSchema()));
      case STRUCT:
        return GenericType.of(Struct.class);
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized Kafka field type: %s", fieldType.type().getName()));
    }
  }
}
