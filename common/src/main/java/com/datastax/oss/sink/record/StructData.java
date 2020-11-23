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

import com.datastax.oss.sink.EngineAPIAdapter;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StructData<EngineRecord, EngineSchema, EngineStruct, EngineField, EngineHeader>
    implements KeyOrValue {

  private final EngineStruct struct;
  private final Set<String> fields;
  private final EngineAPIAdapter<
          EngineRecord, EngineSchema, EngineStruct, EngineField, EngineHeader>
      adapter;

  public StructData(
      @Nullable EngineStruct struct,
      EngineAPIAdapter<EngineRecord, EngineSchema, EngineStruct, EngineField, EngineHeader>
          adapter) {
    this.struct = struct;
    this.adapter = adapter;
    if (struct == null) {
      fields = Collections.singleton(RawData.FIELD_NAME);
    } else {
      fields = new HashSet<>();
      fields.add(RawData.FIELD_NAME);
      fields.addAll(adapter.fields(struct));
    }
  }

  @Override
  public Set<String> fields() {
    return fields;
  }

  @Override
  public Object getFieldValue(String field) {
    if (field.equals(RawData.FIELD_NAME)) {
      return struct;
    }

    if (struct == null) {
      return null;
    }

    Object value = adapter.fieldValue(struct, field);
    if (value instanceof byte[]) {
      // The driver requires a ByteBuffer rather than byte[] when inserting a blob.
      return ByteBuffer.wrap((byte[]) value);
    }
    return value;
  }
}
