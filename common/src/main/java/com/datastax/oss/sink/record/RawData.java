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
package com.datastax.oss.sink.record;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Set;

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
