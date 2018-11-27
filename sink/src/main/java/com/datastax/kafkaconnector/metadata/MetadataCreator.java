/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metadata;

import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

import com.datastax.kafkaconnector.record.JsonData;
import com.datastax.kafkaconnector.record.KeyOrValue;
import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.kafkaconnector.record.StructData;
import com.datastax.kafkaconnector.record.StructDataMetadata;
import com.datastax.kafkaconnector.util.CheckedFunction;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class MetadataCreator {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JavaType JSON_NODE_MAP_TYPE =
      OBJECT_MAPPER.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());
  private static final RecordMetadata JSON_RECORD_METADATA =
      (field, cqlType) ->
          field.equals(RawData.FIELD_NAME) ? GenericType.STRING : GenericType.of(JsonNode.class);
  private static final RawData NULL_DATA = new RawData(null);

  static {
    // Configure the json object mapper
    OBJECT_MAPPER.configure(USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  /**
   * Create a metadata object describing the structure of the given key or value (extracted from a
   * {@link SinkRecord} and a data object that homogenizes interactions with the given key/value
   * (e.g. an implementation of {@link KeyOrValue}).
   *
   * @param keyOrValue the key or value
   * @return a pair of (RecordMetadata, KeyOrValue)
   * @throws IOException if keyOrValue is a String and JSON parsing fails in some unknown way. It's
   *     unclear if this exception can ever trigger in the context of this Connector.
   */
  public static InnerDataAndMetadata makeMeta(Object keyOrValue) throws IOException {
    if (keyOrValue instanceof Struct) {
      Struct innerRecordStruct = (Struct) keyOrValue;
      // TODO: PERF: Consider caching these metadata objects, keyed on schema.
      return new InnerDataAndMetadata(
          new StructData(innerRecordStruct), new StructDataMetadata(innerRecordStruct.schema()));
    } else if (keyOrValue instanceof String) {
      return handleJsonRecord(keyOrValue, (k) -> (String) k);
    } else if (keyOrValue instanceof Map) {
      return handleJsonRecord(keyOrValue, OBJECT_MAPPER::writeValueAsString);
    } else if (keyOrValue != null) {
      KeyOrValue innerData = new RawData(keyOrValue);
      return new InnerDataAndMetadata(innerData, (RecordMetadata) innerData);
    } else {
      // The key or value is null
      return new InnerDataAndMetadata(NULL_DATA, NULL_DATA);
    }
  }

  private static InnerDataAndMetadata handleJsonRecord(
      Object originalRecord, CheckedFunction<Object, String> recordTransformer) throws IOException {
    try {
      KeyOrValue innerData =
          new JsonData(OBJECT_MAPPER, JSON_NODE_MAP_TYPE, recordTransformer.apply(originalRecord));
      return new InnerDataAndMetadata(innerData, JSON_RECORD_METADATA);
    } catch (RuntimeException e) {
      // Json parsing failed. Treat as raw string.
      RawData rawData = new RawData(originalRecord);
      return new InnerDataAndMetadata(rawData, rawData);
    }
  }
}
