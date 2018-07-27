/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonDataTest {

  @Test
  void should_parse_json() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JavaType type = mapper.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());
    JsonData jsonData =
        new JsonData(mapper, type, "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}");
    assertThat(jsonData.fields()).containsOnly(RawRecord.FIELD_NAME, "f1", "f2");
    assertThat(jsonData.getFieldValue("f1")).isEqualTo(new IntNode(42));
    Object f2 = jsonData.getFieldValue("f2");

    Map<String, JsonNode> expectedF2 =
        ImmutableMap.<String, JsonNode>builder()
            .put("sub1", new IntNode(37))
            .put("sub2", new IntNode(96))
            .build();
    assertThat(f2).isEqualTo(new ObjectNode(mapper.getNodeFactory(), expectedF2));
  }

  @Test
  void should_parse_empty_json() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JavaType type = mapper.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());
    JsonData jsonData = new JsonData(mapper, type, "{}");
    assertThat(jsonData.fields()).containsOnly(RawRecord.FIELD_NAME);
    assertThat(jsonData.getFieldValue("noexist")).isEqualTo(null);
  }
}
