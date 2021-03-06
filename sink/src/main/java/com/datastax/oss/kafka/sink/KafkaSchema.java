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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

/** Schema. */
public class KafkaSchema implements AbstractSchema {

  private static final KafkaSchema INT8 = new KafkaSchema(Schema.INT8_SCHEMA);

  private static final KafkaSchema INT16 = new KafkaSchema(Schema.INT16_SCHEMA);
  private static final KafkaSchema INT32 = new KafkaSchema(Schema.INT32_SCHEMA);
  private static final KafkaSchema INT64 = new KafkaSchema(Schema.INT64_SCHEMA);
  private static final KafkaSchema FLOAT32 = new KafkaSchema(Schema.FLOAT32_SCHEMA);
  private static final KafkaSchema FLOAT64 = new KafkaSchema(Schema.FLOAT64_SCHEMA);
  private static final KafkaSchema BOOLEAN = new KafkaSchema(Schema.BOOLEAN_SCHEMA);
  private static final KafkaSchema STRING = new KafkaSchema(Schema.STRING_SCHEMA);
  private static final KafkaSchema BYTES = new KafkaSchema(Schema.BYTES_SCHEMA);

  /**
   * Build a wrapper around a given Kafka Connect Schema. Wrappers for common types are cached using
   * constants.
   *
   * @param schema
   * @return the wrapped schema
   */
  public static KafkaSchema of(Schema schema) {
    switch (schema.type()) {
      case INT8:
        return INT8;
      case INT16:
        return INT16;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case FLOAT32:
        return FLOAT32;
      case FLOAT64:
        return FLOAT64;
      case BOOLEAN:
        return BOOLEAN;
      case STRING:
        return STRING;
      case BYTES:
        return BYTES;
      default:
        // structs and other non primitive types
        return new KafkaSchema(schema);
    }
  }

  private final Schema schema;
  private final Type type;
  private final List<AbstractField> fields;

  private KafkaSchema(Schema schema) {
    this.schema = schema;
    this.type = convertType(schema.type());

    if (schema.type() != Schema.Type.STRUCT) {
      fields = Collections.emptyList();
    } else {
      final List<Field> schemaFields = schema.fields();
      fields = new ArrayList(schemaFields.size());
      for (Field f : schemaFields) {
        fields.add(new KafkaField(f));
      }
    }
  }

  @Override
  public AbstractSchema valueSchema() {
    return of(schema.valueSchema());
  }

  @Override
  public AbstractSchema keySchema() {
    return of(schema.keySchema());
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<? extends AbstractField> fields() {
    return fields;
  }

  @Override
  public AbstractField field(String name) {
    return fields.stream().filter(f -> f.name().equals(name)).findAny().orElse(null);
  }

  private static Type convertType(Schema.Type type) {
    switch (type) {
      case INT8:
        return Type.INT8;
      case INT16:
        return Type.INT16;
      case INT32:
        return Type.INT32;
      case INT64:
        return Type.INT64;
      case FLOAT32:
        return Type.FLOAT32;
      case FLOAT64:
        return Type.FLOAT64;
      case BOOLEAN:
        return Type.BOOLEAN;
      case STRING:
        return Type.STRING;
      case BYTES:
        return Type.BYTES;
      case ARRAY:
        return Type.ARRAY;
      case MAP:
        return Type.MAP;
      case STRUCT:
        return Type.STRUCT;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }

  @Override
  public String toString() {
    return "KafkaSchema{" + "schema=" + schema + ", type=" + type + ", fields=" + fields + '}';
  }
}
