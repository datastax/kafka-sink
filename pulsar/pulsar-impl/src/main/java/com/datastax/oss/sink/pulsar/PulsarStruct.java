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
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractStruct;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/** Wrapper for Pulsar GenericRecord. */
public class PulsarStruct implements AbstractStruct {

  private final Record<GenericRecord> record;
  private final PulsarSchema schema;
  private final LocalSchemaRegistry schemaRegistry;

  public static Object wrap(Object o, LocalSchemaRegistry schemaRegistry) {
    if (o instanceof Record) {
      Record<GenericRecord> record = (Record<GenericRecord>) o;
      return ofRecord(record, schemaRegistry);
    }
    return o;
  }

  public static PulsarStruct ofRecord(
      Record<GenericRecord> record, LocalSchemaRegistry schemaRegistry) {
    PulsarSchema schema = schemaRegistry.ensureAndUpdateSchema(record);
    return new PulsarStruct(record, schema, schemaRegistry);
  }

  public PulsarStruct(
      Record<GenericRecord> record, PulsarSchema schema, LocalSchemaRegistry schemaRegistry) {
    this.record = record;
    this.schemaRegistry = schemaRegistry;
    this.schema = schema;
  }

  @Override
  public Object get(String field) {
    return wrap(record.getValue().getField(field), schemaRegistry);
  }

  @Override
  public AbstractSchema schema() {
    return schema;
  }
}
