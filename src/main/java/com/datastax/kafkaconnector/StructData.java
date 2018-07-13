/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.Nullable;

public class StructData implements Record {

  private final Struct struct;
  private final Set<String> fields;

  StructData(@Nullable Struct struct) {
    this.struct = struct;
    fields =
        struct == null
            ? Collections.emptySet()
            : struct.schema().fields().stream().map(Field::name).collect(Collectors.toSet());
  }

  @Override
  public Set<String> fields() {
    return fields;
  }

  @Override
  public Object getFieldValue(String field) {
    if (struct == null) {
      return null;
    }

    Object value = struct.get(field);
    if (value instanceof byte[]) {
      // The driver requires a ByteBuffer rather than byte[] when inserting a blob.
      return ByteBuffer.wrap((byte[]) value);
    }
    return value;
  }
}
