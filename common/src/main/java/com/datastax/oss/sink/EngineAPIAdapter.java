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

/**
 * Provides access to schema and data elements of specific source.
 *
 * @param <EngineRecord>
 * @param <EngineSchema>
 * @param <EngineStruct>
 * @param <EngineField>
 * @param <EngineHeader>
 */
public interface EngineAPIAdapter<
    EngineRecord, EngineSchema, EngineStruct, EngineField, EngineHeader> {

  RuntimeException adapt(ConfigException ex);

  RuntimeException adapt(RetriableException ex);

  /**
   * @param record
   * @return topic of the record has come from
   */
  String topic(EngineRecord record);

  /**
   * @param record
   * @return key of the record
   */
  Object key(EngineRecord record);

  /**
   * @param record
   * @return value of the record
   */
  Object value(EngineRecord record);

  /**
   * @param record
   * @return headers accompanying the record
   */
  Set<EngineHeader> headers(EngineRecord record);

  /**
   * @param header
   * @return key of the header
   */
  String headerKey(EngineHeader header);

  /**
   * @param header
   * @return value of the record
   */
  Object headerValue(EngineHeader header);

  /**
   * @param header
   * @return schema of the header's value
   */
  EngineSchema headerSchema(EngineHeader header);

  /**
   * @param record
   * @return timestamp of the record
   */
  Long timestamp(EngineRecord record);

  /**
   * @param object
   * @return whether the object is struct
   */
  boolean isStruct(Object object);

  /**
   * @param struct
   * @return schema of the struct
   */
  EngineSchema schema(EngineStruct struct);

  /**
   * @param struct
   * @return names of the struct's fields
   */
  Set<String> fields(EngineStruct struct);

  /**
   * @param struct
   * @param fieldName
   * @return values of the specific struct's field
   */
  Object fieldValue(EngineStruct struct, String fieldName);

  /**
   * @param schema
   * @param fieldName
   * @return schema of the specific schema's field
   */
  EngineSchema fieldSchema(EngineSchema schema, String fieldName);

  /**
   * Converts specified schema to a common schema type.
   *
   * @param schema
   * @return common schema type
   */
  SchemaSupport.Type type(EngineSchema schema);

  /**
   * @param schema
   * @return
   */
  EngineSchema valueSchema(EngineSchema schema);

  EngineSchema keySchema(EngineSchema schema);

  Class<EngineStruct> structClass();
}
