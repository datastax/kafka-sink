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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Metadata associated with a {@link KeyValueRecord}. */
public class KeyValueRecordMetadata implements RecordMetadata {

  private final RecordMetadata keyMetadata;
  private final RecordMetadata valueMetadata;

  public KeyValueRecordMetadata(
      @Nullable RecordMetadata keyMetadata, @Nullable RecordMetadata valueMetadata) {
    this.keyMetadata = keyMetadata;
    this.valueMetadata = valueMetadata;
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    if (field.startsWith("key.")) {
      return keyMetadata != null ? keyMetadata.getFieldType(field.substring(4), cqlType) : null;
    } else if (field.startsWith("value.")) {
      return valueMetadata != null ? valueMetadata.getFieldType(field.substring(6), cqlType) : null;
    } else {
      assert false : "field name must start with 'key.' or 'value.'.";
    }
    return null;
  }
}
