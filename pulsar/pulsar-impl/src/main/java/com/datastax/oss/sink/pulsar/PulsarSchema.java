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

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Datatype. */
public class PulsarSchema implements AbstractSchema {

  private static final PulsarSchema INT8 = new PulsarSchema(AbstractSchema.Type.INT8);
  private static final PulsarSchema INT16 = new PulsarSchema(AbstractSchema.Type.INT16);
  private static final PulsarSchema INT32 = new PulsarSchema(AbstractSchema.Type.INT32);
  private static final PulsarSchema INT64 = new PulsarSchema(AbstractSchema.Type.INT64);
  private static final PulsarSchema FLOAT32 = new PulsarSchema(AbstractSchema.Type.FLOAT32);
  private static final PulsarSchema FLOAT64 = new PulsarSchema(AbstractSchema.Type.FLOAT64);
  private static final PulsarSchema BOOLEAN = new PulsarSchema(AbstractSchema.Type.BOOLEAN);
  private static final PulsarSchema STRING = new PulsarSchema(AbstractSchema.Type.STRING);
  private static final PulsarSchema BYTES = new PulsarSchema(AbstractSchema.Type.BYTES);

  private static final Logger log = LoggerFactory.getLogger(PulsarSchema.class);

  public static PulsarSchema of(String path, Object value, LocalSchemaRegistry registry) {
    if (value == null) {
      // there is no support for NULLS in Cassandra Driver type system
      // using STRING
      return STRING;
    }
    if (value instanceof Integer) {
      return INT32;
    }
    if (value instanceof String) {
      return STRING;
    }
    if (value instanceof Long) {
      return INT64;
    }
    if (value instanceof byte[]) {
      return BYTES;
    }
    if (value instanceof Boolean) {
      return BOOLEAN;
    }
    if (value instanceof Byte) {
      return INT8;
    }
    if (value instanceof Float) {
      return FLOAT32;
    }
    if (value instanceof Double) {
      return FLOAT64;
    }
    if (value instanceof Short) {
      return INT16;
    }
    if (value instanceof GenericRecord) {
      return registry.ensureAndUpdateSchema(path, (GenericRecord) value);
    }
    throw new UnsupportedOperationException("type " + value.getClass());
  }

  static PulsarSchema createFromStruct(
      String path, GenericRecord value, LocalSchemaRegistry registry) {
    return new PulsarSchema(path, value, registry);
  }

  private final Map<String, PulsarField> fields;
  private final AbstractSchema.Type type;

  private PulsarSchema(String path, GenericRecord template, LocalSchemaRegistry registry) {
    this.fields = new ConcurrentHashMap<>();
    this.type = AbstractSchema.Type.STRUCT;
    update(path, template, registry);
  }

  private PulsarSchema(AbstractSchema.Type type) {
    this.type = type;
    this.fields = Collections.emptyMap();
  }

  /**
   * Unfortunately Pulsar does not return information about the datatype of Fields, so we have to
   * understand it using Java reflection.
   *
   * @param path context definition for the schema (root record or subfield)
   * @param template an instance.
   */
  public final void update(String path, GenericRecord template, LocalSchemaRegistry registry) {
    for (Field f : template.getFields()) {
      Object value = template.getField(f);
      // in case of null value we are going to use
      // a dummy (string) type
      // at the first occourrance of a non-null value
      // we are updating the type
      // it is not expected that a field changes data type
      // once we find a non-null value
      PulsarSchema schemaForField = PulsarSchema.of(path + "." + f.getName(), value, registry);
      PulsarField field = new PulsarField(f.getName(), schemaForField);
      this.fields.put(f.getName(), field);
    }
  }

  @Override
  public AbstractSchema valueSchema() {
    return this;
  }

  @Override
  public AbstractSchema keySchema() {
    // in Pulsar we only have String keys
    return STRING;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<? extends AbstractField> fields() {
    return new ArrayList<>(fields.values());
  }

  @Override
  public AbstractField field(String name) {
    return fields.get(name);
  }

  @Override
  public String toString() {
    return "PulsarSchema{" + "fields=" + fields + ", type=" + type + '}';
  }
}
