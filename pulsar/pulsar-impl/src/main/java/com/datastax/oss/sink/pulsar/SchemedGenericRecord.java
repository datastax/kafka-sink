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

import com.datastax.oss.sink.util.Tuple2;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class SchemedGenericRecord {

  private GenericRecord record;
  private Schema schema;

  public SchemedGenericRecord(GenericRecord record, Schema schema) {
    this.record = record;
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  public Set<String> getFields() {
    return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
  }

  public Object getField(String fieldName) {
    Object val = record.getField(fieldName);
    if (val instanceof GenericRecord) {
      val = new SchemedGenericRecord((GenericRecord) val, schema.getField(fieldName).schema());
    } else if (val instanceof Map) {
      return castKeys((Map<?, ?>) val);
    } else if ("null".equals(val)) val = null;
    return val;
  }

  public static Map<String, ?> castKeys(Map<?, ?> map) {
    return ((Map<?, ?>) map)
        .entrySet()
        .stream()
        .map(
            e ->
                Tuple2.of(
                    String.valueOf(e.getKey()),
                    e.getValue() instanceof Map
                        ? castKeys((Map<?, ?>) e.getValue())
                        : e.getValue()))
        .collect(HashMap::new, (m, t) -> m.put(t._1, t._2), HashMap::putAll);
  }
}
