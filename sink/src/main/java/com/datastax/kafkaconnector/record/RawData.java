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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;

/** The key or value of a {@link SinkRecord} when it is a primitive type. */
public class RawData implements KeyOrValue, RecordMetadata {
  public static final String FIELD_NAME = "__self";
  public static final String VALUE_FIELD_NAME = "value." + FIELD_NAME;
  private static final ImmutableSet<String> FIELDS = ImmutableSet.of(FIELD_NAME);

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
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
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
