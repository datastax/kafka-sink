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
package com.datastax.oss.sink;

import com.datastax.oss.sink.config.ConfigException;
import com.datastax.oss.sink.record.SchemaSupport;
import java.util.Set;

public interface EngineAPIAdapter<
    EngineRecord, EngineSchema, EngineStruct, EngineField, EngineHeader> {

  RuntimeException adapt(ConfigException ex);

  RuntimeException adapt(RetriableException ex);

  String topic(EngineRecord record);

  Object key(EngineRecord record);

  Object value(EngineRecord record);

  Set<EngineHeader> headers(EngineRecord record);

  String headerKey(EngineHeader header);

  Object headerValue(EngineHeader header);

  EngineSchema headerSchema(EngineHeader header);

  Long timestamp(EngineRecord record);

  boolean isStruct(Object object);

  EngineSchema schema(EngineStruct struct);

  Set<String> fields(EngineStruct struct);

  Object fieldValue(EngineStruct struct, String fieldName);

  EngineSchema fieldSchema(EngineSchema schema, String fieldSchema);

  SchemaSupport.Type type(EngineSchema schema);

  EngineSchema valueSchema(EngineSchema schema);

  EngineSchema keySchema(EngineSchema schema);

  Class<EngineStruct> structClass();
}
