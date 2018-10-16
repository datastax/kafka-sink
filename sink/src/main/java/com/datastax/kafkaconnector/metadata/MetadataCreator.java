package com.datastax.kafkaconnector.metadata;

import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.kafkaconnector.record.JsonData;
import com.datastax.kafkaconnector.record.KeyOrValue;
import com.datastax.kafkaconnector.record.MapData;
import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.kafkaconnector.record.StructData;
import com.datastax.kafkaconnector.record.StructDataMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

public class MetadataCreator {
  private static final Logger log = LoggerFactory.getLogger(MetadataCreator.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JavaType JSON_NODE_MAP_TYPE =
      OBJECT_MAPPER.constructType(new TypeReference<Map<String, JsonNode>>() {
      }.getType());
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
   *                     unclear if this exception can ever trigger in the context of this Connector.
   */
  public static InnerDataAndMetadata makeMeta(Object keyOrValue) throws IOException {
    KeyOrValue innerData;
    RecordMetadata innerMetadata;
    if (keyOrValue != null) {
      log.info("instanceOf keyOrValue: {} is: {}", keyOrValue, keyOrValue.getClass());
    }
    if (keyOrValue instanceof Struct) {
      Struct innerRecordStruct = (Struct) keyOrValue;
      // TODO: PERF: Consider caching these metadata objects, keyed on schema.
      innerMetadata = new StructDataMetadata(innerRecordStruct.schema());
      innerData = new StructData(innerRecordStruct);
    } else if (keyOrValue instanceof String) {
      innerMetadata = JSON_RECORD_METADATA;
      try {
        innerData = new JsonData(OBJECT_MAPPER, JSON_NODE_MAP_TYPE, (String) keyOrValue);
      } catch (RuntimeException e) {
        // Json parsing failed. Treat as raw string.
        innerData = new RawData(keyOrValue);
        innerMetadata = (RecordMetadata) innerData;
      }
    } else if (keyOrValue instanceof Map) {
      innerData = MapData.fromMap((Map)keyOrValue);
    } else if (keyOrValue != null) {
      innerData = new RawData(keyOrValue);
      innerMetadata = (RecordMetadata) innerData;
    } else {
      // The key or value is null
      innerData = NULL_DATA;
      innerMetadata = NULL_DATA;
    }
    return new InnerDataAndMetadata(innerData, innerMetadata);
  }


}
