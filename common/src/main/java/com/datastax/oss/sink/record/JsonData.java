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
package com.datastax.oss.sink.record;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** The key or value of a record when it is a JSON string. */
public class JsonData implements KeyOrValue {
  private final Map<String, JsonNode> data;
  private final String json;
  private final Set<String> fields;

  public JsonData(ObjectMapper objectMapper, JavaType jsonNodeMapType, String json)
      throws IOException {
    this.json = json;
    JsonFactory factory = objectMapper.getFactory();
    if (json == null) {
      data = Collections.emptyMap();
    } else {
      try (JsonParser parser = factory.createParser(json)) {
        MappingIterator<JsonNode> it = objectMapper.readValues(parser, JsonNode.class);
        if (it.hasNext()) {
          JsonNode node = it.next();
          data = objectMapper.convertValue(node, jsonNodeMapType);
        } else {
          data = Collections.emptyMap();
        }
      }
    }
    fields = new HashSet<>(data.keySet());
    fields.add(RawData.FIELD_NAME);
  }

  @Override
  public Set<String> fields() {
    return fields;
  }

  @Override
  public Object getFieldValue(String field) {
    if (field.equals(RawData.FIELD_NAME)) {
      return json;
    }
    return data.get(field);
  }
}
