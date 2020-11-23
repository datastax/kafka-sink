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

import static com.datastax.oss.sink.record.StructDataMetadataSupport.*;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.sink.EngineAPIAdapter;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Metadata associated with a {@link StructData}. */
public class StructDataMetadata<EngineSchema> implements RecordMetadata {
  private final EngineSchema schema;
  private final EngineAPIAdapter<?, EngineSchema, ?, ?, ?> adapter;

  public StructDataMetadata(
      @NonNull EngineSchema schema, EngineAPIAdapter<?, EngineSchema, ?, ?, ?> adapter) {
    this.schema = schema;
    this.adapter = adapter;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    if (field.equals(RawData.FIELD_NAME)) {
      return GenericType.of(adapter.structClass());
    }
    EngineSchema fieldType = adapter.fieldSchema(schema, field);
    return getGenericType(fieldType, adapter);
  }
}
