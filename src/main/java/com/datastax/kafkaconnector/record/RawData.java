/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

/** The key or value of a {@link SinkRecord} when it is a primitive type. */
public class RawData implements KeyOrValue, RecordMetadata {
  public static final String FIELD_NAME = "__self";
  private static final Set<String> FIELDS = new HashSet<>();

  static {
    FIELDS.add(FIELD_NAME);
  }

  private final GenericType<?> type;
  private final Object value;

  public RawData(Object keyOrValue) {
    // The driver requires a ByteBuffer rather than byte[] when inserting a blob.
    value = keyOrValue instanceof byte[] ? ByteBuffer.wrap((byte[]) keyOrValue) : keyOrValue;

    if (value != null) {
      type =
          value instanceof ByteBuffer ? GenericType.BYTE_BUFFER : GenericType.of(value.getClass());
    } else {
      type = GenericType.STRING;
    }
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    return type;
  }

  @Override
  public Set<String> fields() {
    return FIELDS;
  }

  @Override
  public Object getFieldValue(String field) {
    return value;
  }
}
