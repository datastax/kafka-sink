/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

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
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The key or value of a {@link SinkRecord} when it is a JSON string. */
public class JsonData implements KeyOrValue {
  private static final Logger log = LoggerFactory.getLogger(JsonData.class);
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
    log.info("get for json field: {}, json: {}", field, json);
    if (field.equals(RawData.FIELD_NAME)) {
      return json;
    }
    return data.get(field);
  }
}
