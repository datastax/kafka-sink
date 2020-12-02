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
package com.datastax.oss.sink.pulsar.gen;

import com.datastax.oss.sink.record.SchemaSupport;
import com.datastax.oss.sink.util.Tuple2;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class GenSchema {

  public final SchemaSupport.Type type;

  private GenSchema(SchemaSupport.Type type) {
    this.type = type;
  }

  public static GenValue adaptValue(Object value) {
    if (value == null) return GenValue.NULL;
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      StructGenSchema schema = new StructGenSchema(record.getFields().size());
      Map<String, GenValue> values =
          record
              .getFields()
              .stream()
              .map(
                  field -> {
                    GenValue genValue = adaptValue(record.getField(field));
                    schema.addField(field.getName(), genValue.schema);
                    return Tuple2.of(field.getName(), genValue);
                  })
              .collect(HashMap::new, (m, t) -> m.put(t._1, t._2), HashMap::putAll);
      return new GenValue.GenStruct(values, schema);
    } else if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, ?> map = (Map<String, ?>) value;
      if (map.isEmpty()) return new GenValue<>(value, new MapGenSchema(GenSchema.NULL));
      Map<String, GenValue> val =
          map.entrySet()
              .stream()
              .map(et -> Tuple2.of(et.getKey(), adaptValue(et.getValue())))
              .collect(HashMap::new, (m, t) -> m.put(t._1, t._2), HashMap::putAll);
      return new GenValue<>(val, new MapGenSchema(val.values().iterator().next().schema));
    } else if (value instanceof List) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) return new GenValue<>(value, new ArrayGenSchema(GenSchema.NULL));
      List<GenValue> val = list.stream().map(GenSchema::adaptValue).collect(Collectors.toList());
      return new GenValue<>(val, new ArrayGenSchema(val.get(0).schema));
    } else if (value instanceof String) {
      return new GenValue<>(value, STRING);
    } else if (value instanceof Utf8) {
      return new GenValue<>(value.toString(), STRING);
    } else if (value instanceof Integer) {
      return new GenValue<>(value, INT32);
    } else if (value instanceof Long) {
      return new GenValue<>(value, INT64);
    } else if (value instanceof Float) {
      return new GenValue<>(value, FLOAT32);
    } else if (value instanceof Double) {
      return new GenValue<>(value, FLOAT64);
    } else if (value instanceof Boolean) {
      return new GenValue<>(value, BOOLEAN);
    } else if (value instanceof byte[]) {
      return new GenValue<>(value, BYTES);
    } else if (value instanceof ByteBuffer) {
      return new GenValue<>(((ByteBuffer) value).array(), BYTES);
    }
    throw new IllegalArgumentException(
        String.format("could not infer schema of (%s) %s", value.getClass().getName(), value));
  }

  public static class StructGenSchema extends GenSchema {
    private StructGenSchema(int fieldNum) {
      super(SchemaSupport.Type.STRUCT);
      fields = new HashMap<>(fieldNum);
    }

    private final Map<String, GenSchema> fields;

    private void addField(String name, GenSchema schema) {
      fields.put(name, schema);
    }

    public GenSchema field(String fieldName) {
      return fields.get(fieldName);
    }

    public Set<String> fields() {
      return fields.keySet();
    }
  }

  public abstract static class CollectionGenSchema extends GenSchema {
    private final GenSchema elementSchema;

    private CollectionGenSchema(SchemaSupport.Type type, GenSchema elementSchema) {
      super(type);
      this.elementSchema = elementSchema;
    }

    public GenSchema elementSchema() {
      return elementSchema;
    }
  }

  public static class ArrayGenSchema extends CollectionGenSchema {
    private ArrayGenSchema(GenSchema elementSchema) {
      super(SchemaSupport.Type.ARRAY, elementSchema);
    }
  }

  public static class MapGenSchema extends CollectionGenSchema {
    private MapGenSchema(GenSchema valueSchema) {
      super(SchemaSupport.Type.MAP, valueSchema);
    }

    public static final GenSchema KEY_SCHEMA = STRING;
  }

  public static final GenSchema BOOLEAN = new GenSchema(SchemaSupport.Type.BOOLEAN);
  public static final GenSchema BYTES = new GenSchema(SchemaSupport.Type.BYTES);
  public static final GenSchema FLOAT32 = new GenSchema(SchemaSupport.Type.FLOAT32);
  public static final GenSchema FLOAT64 = new GenSchema(SchemaSupport.Type.FLOAT64);
  public static final GenSchema INT32 = new GenSchema(SchemaSupport.Type.INT32);
  public static final GenSchema INT64 = new GenSchema(SchemaSupport.Type.INT64);
  public static final GenSchema STRING = new GenSchema(SchemaSupport.Type.STRING);
  public static final GenSchema NULL = new GenSchema(SchemaSupport.Type.NULL);
}
