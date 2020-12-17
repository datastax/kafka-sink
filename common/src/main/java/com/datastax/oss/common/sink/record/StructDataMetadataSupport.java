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
package com.datastax.oss.common.sink.record;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractStruct;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;

public class StructDataMetadataSupport {
  private static final ImmutableMap<AbstractSchema.Type, GenericType<?>> TYPE_MAP =
      ImmutableMap.<AbstractSchema.Type, GenericType<?>>builder()
          .put(AbstractSchema.Type.BOOLEAN, GenericType.BOOLEAN)
          .put(AbstractSchema.Type.FLOAT64, GenericType.DOUBLE)
          .put(AbstractSchema.Type.INT64, GenericType.LONG)
          .put(AbstractSchema.Type.FLOAT32, GenericType.FLOAT)
          .put(AbstractSchema.Type.INT8, GenericType.BYTE)
          .put(AbstractSchema.Type.INT16, GenericType.SHORT)
          .put(AbstractSchema.Type.INT32, GenericType.INTEGER)
          .put(AbstractSchema.Type.STRING, GenericType.STRING)
          .put(AbstractSchema.Type.BYTES, GenericType.BYTE_BUFFER)
          .build();

  @NonNull
  static GenericType<?> getGenericType(@NonNull AbstractSchema fieldType) {
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
        return GenericType.of(AbstractStruct.class);
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized Kafka field type: %s", fieldType.type().getName()));
    }
  }
}
