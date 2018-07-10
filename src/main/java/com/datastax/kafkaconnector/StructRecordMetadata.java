/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.LIST;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TUPLE;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.jetbrains.annotations.NotNull;

class StructRecordMetadata implements RecordMetadata {
  private static final ImmutableMap<Schema, GenericType<?>> TYPE_MAP =
      ImmutableMap.<Schema, GenericType<?>>builder()
          .put(Schema.BOOLEAN_SCHEMA, GenericType.BOOLEAN)
          .put(Schema.FLOAT64_SCHEMA, GenericType.DOUBLE)
          .put(Schema.INT64_SCHEMA, GenericType.LONG)
          .put(Schema.FLOAT32_SCHEMA, GenericType.FLOAT)
          .put(Schema.INT8_SCHEMA, GenericType.BYTE)
          .put(Schema.INT16_SCHEMA, GenericType.SHORT)
          .put(Schema.INT32_SCHEMA, GenericType.INTEGER)
          .put(Schema.STRING_SCHEMA, GenericType.STRING)
          .put(Schema.BYTES_SCHEMA, GenericType.BYTE_BUFFER)
          .build();
  private final Schema schema;

  StructRecordMetadata(@NotNull Schema schema) {
    this.schema = schema;
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    Schema fieldType = schema.field(field).schema();
    return getGenericType(fieldType, cqlType);
  }

  @NotNull
  private GenericType<?> getGenericType(@NotNull Schema fieldType, @NotNull DataType cqlType) {
    GenericType<?> result = TYPE_MAP.get(fieldType);
    if (result != null) {
      return result;
    }
    // This is a complex type.
    switch (fieldType.type()) {
      case ARRAY:
        switch(cqlType.getProtocolCode()) {
          case SET:
            return GenericType.listOf(getGenericType(fieldType.valueSchema(), ((SetType) cqlType).getElementType()));
          case LIST:
            return GenericType.listOf(getGenericType(fieldType.valueSchema(), ((ListType) cqlType).getElementType()));
          case TUPLE:
            return GenericType.TUPLE_VALUE;
        }
      case MAP:
        return GenericType.mapOf(getGenericType(fieldType.keySchema(), ((MapType) cqlType).getKeyType()), getGenericType(fieldType.valueSchema(), ((MapType) cqlType).getValueType()));
      default:
        throw new IllegalArgumentException(String.format("Unrecognized Kafka field type: %s", fieldType.type().getName()));
    }
  }
}
