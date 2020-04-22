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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** Metadata associated with a {@link KeyValueRecord}. */
public class KeyValueRecordMetadata implements RecordMetadata {

  private final RecordMetadata keyMetadata;
  private final RecordMetadata valueMetadata;
  private RecordMetadata headersMetadata;

  public KeyValueRecordMetadata(
      @Nullable RecordMetadata keyMetadata,
      @Nullable RecordMetadata valueMetadata,
      @Nullable RecordMetadata headersMetadata) {
    this.keyMetadata = keyMetadata;
    this.valueMetadata = valueMetadata;
    this.headersMetadata = headersMetadata;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    if (field.startsWith("key.")) {
      return keyMetadata != null ? keyMetadata.getFieldType(field.substring(4), cqlType) : null;
    } else if (field.startsWith("value.")) {
      return valueMetadata != null ? valueMetadata.getFieldType(field.substring(6), cqlType) : null;
    } else if (field.startsWith("header.")) {
      return headersMetadata != null
          ? headersMetadata.getFieldType(field.substring(7), cqlType)
          : null;
    } else {
      throw new IllegalArgumentException(
          "field name must start with 'key.', 'value.' or 'header.'.");
    }
  }
}
