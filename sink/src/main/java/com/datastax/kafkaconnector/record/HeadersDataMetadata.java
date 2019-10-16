/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import static com.datastax.kafkaconnector.record.StructDataMetadataSupport.getGenericType;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.jetbrains.annotations.NotNull;

/** Metadata associated with a {@link StructData}. */
public class HeadersDataMetadata implements RecordMetadata {
  private final Headers headers;

  public HeadersDataMetadata(Headers headers) {
    this.headers = headers;
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    for (Header h : headers) {
      if (h.key().equals(field)) {
        return getGenericType(h.schema());
      }
    }
    throw new IllegalArgumentException(
        "The field: " + field + " is not present in the record headers: " + headers);
  }
}
