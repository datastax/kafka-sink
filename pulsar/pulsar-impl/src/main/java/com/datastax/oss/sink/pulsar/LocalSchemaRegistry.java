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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * This is a local cache of PulsarSchema. Unfortunately Pulsar does not return information about the
 * datatype of Fields, so we have to understand it using Java reflection.
 */
public class LocalSchemaRegistry {

  private final ConcurrentHashMap<String, PulsarSchema> registry = new ConcurrentHashMap<>();

  public PulsarSchema ensureAndUpdateSchema(Record<GenericRecord> struct) {
    String path = computeRecordSchemaPath(struct);
    // for nested structures we are going to add the name of the field
    return ensureAndUpdateSchema(path, struct.getValue());
  }

  public static String computeRecordSchemaPath(Record<GenericRecord> struct) {
    String schemaDef = "?";
    // versions of Pulsar prior to 2.6.3 do not report schema information
    if (struct.getSchema() != null && struct.getSchema().getSchemaInfo() != null) {
      schemaDef = struct.getSchema().getSchemaInfo().getSchemaDefinition();
    } // we using as key the fully qualified topic name + string (JSON) representation of the schema
    // this way we are supporting schema evolution easily
    String path = struct.getTopicName().orElse(null) + schemaDef;
    return path;
  }

  public PulsarSchema ensureAndUpdateSchema(String path, GenericRecord struct) {
    PulsarSchema res =
        registry.computeIfAbsent(
            path,
            s -> {
              return PulsarSchema.createFromStruct(path, struct, this);
            });
    // need to recover nulls from previous records
    res.update(path, struct, this);
    return res;
  }
}
