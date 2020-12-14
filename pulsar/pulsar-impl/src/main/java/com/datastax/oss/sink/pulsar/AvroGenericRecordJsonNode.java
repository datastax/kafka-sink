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

import com.datastax.oss.sink.pulsar.util.DataReader;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroGenericRecordJsonNode implements GenericRecord {

  private final Schema schema;
  private final JsonNode node;

  public AvroGenericRecordJsonNode(Schema schema, JsonNode node) {
    this.schema = schema;
    this.node = node;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void put(String key, Object v) {
    throw new UnsupportedOperationException("record is readonly");
  }

  @Override
  public Object get(String key) {
    return adjustValue(node.get(key), key);
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException("record is readonly");
  }

  @Override
  public Object get(int i) {
    return adjustValue(node.get(i), i);
  }

  private Object adjustValue(JsonNode valueNode, String fieldName) {
    return valueNode.isObject()
        ? new AvroGenericRecordJsonNode(schema.getField(fieldName).schema(), valueNode)
        : DataReader.primitiveValue(valueNode);
  }

  private Object adjustValue(JsonNode valueNode, int fieldIndex) {
    return valueNode.isObject()
        ? new AvroGenericRecordJsonNode(schema.getFields().get(fieldIndex).schema(), valueNode)
        : DataReader.primitiveValue(valueNode);
  }
}
