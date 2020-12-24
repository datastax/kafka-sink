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
import com.datastax.oss.common.sink.util.SinkUtil;
import java.util.Optional;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/** Wrapper for Pulsar GenericRecord. */
public class PulsarStruct implements AbstractStruct {

  private final GenericRecord record;
  private final Optional<Long> eventTime;
  private final PulsarSchema schema;
  private final String path;
  private final LocalSchemaRegistry schemaRegistry;

  public static Object wrap(
      PulsarStruct parent, String fieldName, Object o, LocalSchemaRegistry schemaRegistry) {

    if (o instanceof GenericRecord) {
      String schemaPath = parent.getPath() + "/" + fieldName;

      return new PulsarStruct(
          (GenericRecord) o,
          parent.eventTime,
          schemaRegistry.ensureAndUpdateSchema(schemaPath, (GenericRecord) o),
          schemaPath,
          schemaRegistry);
    }
    return o;
  }

  public static PulsarStruct ofRecord(
      Record<GenericRecord> record, LocalSchemaRegistry schemaRegistry) {
    PulsarSchema schema = schemaRegistry.ensureAndUpdateSchema(record);
    String path = LocalSchemaRegistry.computeRecordSchemaPath(record);
    return new PulsarStruct(record, schema, path, schemaRegistry);
  }

  public PulsarStruct(
      Record<GenericRecord> record,
      PulsarSchema schema,
      String path,
      LocalSchemaRegistry schemaRegistry) {
    this.record = record.getValue();
    this.eventTime = record.getEventTime();
    this.schemaRegistry = schemaRegistry;
    this.schema = schema;
    this.path = path;
  }

  public PulsarStruct(
      GenericRecord record,
      Optional<Long> eventTime,
      PulsarSchema schema,
      String path,
      LocalSchemaRegistry schemaRegistry) {
    this.record = record;
    this.eventTime = eventTime;
    this.schemaRegistry = schemaRegistry;
    this.schema = schema;
    this.path = path;
  }

  @Override
  public Object get(String field) {
    if (SinkUtil.TIMESTAMP_VARNAME.equals(field)) {
      return eventTime.orElse(null);
    }
    return wrap(this, field, record.getField(field), schemaRegistry);
  }

  @Override
  public AbstractSchema schema() {
    return schema;
  }

  public GenericRecord getRecord() {
    return record;
  }

  public String getPath() {
    return path;
  }
}
