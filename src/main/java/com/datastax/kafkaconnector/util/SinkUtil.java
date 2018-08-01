/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/** Utility class to house useful methods and constants that the rest of the application may use. */
public class SinkUtil {
  public static final String TIMESTAMP_VARNAME = "kafka_internal_timestamp";

  public static @NotNull String serializeTopicMappings(DseSinkConfig config) {
    Map<String, TopicConfig> topicConfigs = config.getTopicConfigs();
    // Make a map of <topic, mapping>, where mapping is a map of <column, field>
    Map<String, Map<String, String>> mappings = new HashMap<>();
    topicConfigs.forEach(
        (topicName, topicConfig) -> {
          Map<String, String> topicMap = mappings.computeIfAbsent(topicName, k -> new HashMap<>());
          topicConfig
              .getMapping()
              .forEach((column, field) -> topicMap.put(column.asInternal(), field.asInternal()));
        });

    // JSONify
    try {
      return new ObjectMapper().writeValueAsString(mappings);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Map<CqlIdentifier, CqlIdentifier>> deserializeTopicMappings(
      String mappings) {
    Map<String, Map<CqlIdentifier, CqlIdentifier>> topicMappings;
    ObjectMapper objectMapper = new ObjectMapper();
    TypeReference<HashMap<String, HashMap<String, String>>> typeReference =
        new TypeReference<HashMap<String, HashMap<String, String>>>() {};
    try {
      Map<String, Map<String, String>> stringMap = objectMapper.readValue(mappings, typeReference);
      topicMappings = new HashMap<>();
      stringMap.forEach(
          (topicName, topicMapping) -> {
            Map<CqlIdentifier, CqlIdentifier> mapping =
                topicMappings.computeIfAbsent(topicName, k -> new HashMap<>());
            topicMapping.forEach(
                (column, field) ->
                    mapping.put(
                        CqlIdentifier.fromInternal(column), CqlIdentifier.fromInternal(field)));
          });
    } catch (IOException e) {
      // This should never happen, since the value is coming from the connector, which generated it
      // with Jackson.
      throw new RuntimeException(e);
    }
    return topicMappings;
  }

  private SinkUtil() {}
}
