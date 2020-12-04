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

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.sink.EngineAPIAdapter;
import edu.umd.cs.findbugs.annotations.NonNull;

public class StructDataMetadataSupport {
  public static final ImmutableMap<SchemaSupport.Type, GenericType<?>> TYPE_MAP =
      ImmutableMap.<SchemaSupport.Type, GenericType<?>>builder()
          .put(SchemaSupport.Type.BOOLEAN, GenericType.BOOLEAN)
          .put(SchemaSupport.Type.FLOAT64, GenericType.DOUBLE)
          .put(SchemaSupport.Type.INT64, GenericType.LONG)
          .put(SchemaSupport.Type.FLOAT32, GenericType.FLOAT)
          .put(SchemaSupport.Type.INT8, GenericType.BYTE)
          .put(SchemaSupport.Type.INT16, GenericType.SHORT)
          .put(SchemaSupport.Type.INT32, GenericType.INTEGER)
          .put(SchemaSupport.Type.STRING, GenericType.STRING)
          .put(SchemaSupport.Type.BYTES, GenericType.BYTE_BUFFER)
          .build();

  @NonNull
  public static <EngineSchema> GenericType<?> getGenericType(
      @NonNull EngineSchema fieldType, EngineAPIAdapter<?, EngineSchema, ?, ?, ?> adapter) {
    GenericType<?> result = TYPE_MAP.get(adapter.type(fieldType));
    if (result != null) {
      return result;
    }
    // This is a complex type.
    // TODO: PERF: Consider caching these results and check the cache before creating
    // new entries.

    switch (adapter.type(fieldType)) {
      case ARRAY:
        return GenericType.listOf(getGenericType(adapter.valueSchema(fieldType), adapter));
      case MAP:
        return GenericType.mapOf(
            getGenericType(adapter.keySchema(fieldType), adapter),
            getGenericType(adapter.valueSchema(fieldType), adapter));
      case STRUCT:
        return GenericType.of(adapter.structClass());
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized field type: %s", adapter.type(fieldType).getName()));
    }
  }
}
