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
import java.util.Set;

/** Metadata associated with a {@link StructData}. */
public class HeadersDataMetadata<EngineSchema, EngineHeader> implements RecordMetadata {
  private final Set<EngineHeader> headers;
  private final EngineAPIAdapter<?, EngineSchema, ?, ?, EngineHeader> adapter;

  public HeadersDataMetadata(
      Set<EngineHeader> headers, EngineAPIAdapter<?, EngineSchema, ?, ?, EngineHeader> adapter) {
    this.headers = headers;
    this.adapter = adapter;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    for (EngineHeader h : headers) {
      if (adapter.headerKey(h).equals(field)) {
        return getGenericType(adapter.headerSchema(h), adapter);
      }
    }
    throw new IllegalArgumentException(
        "The field: " + field + " is not present in the record headers: " + headers);
  }
}
