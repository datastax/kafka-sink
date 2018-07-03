/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class JsonData implements Record {
  private final Map<String, JsonNode> data;

  JsonData(ObjectMapper objectMapper, JavaType jsonNodeMapType, String json) throws IOException {
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
  }

  @Override
  public Set<String> fields() {
    return data.keySet();
  }

  @Override
  public Object getFieldValue(String field) {
    return data.get(field);
  }
}
