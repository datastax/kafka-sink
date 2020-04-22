/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class MetadataCreatorTest {

  private static final PrimitiveType CQL_TYPE = new PrimitiveType(-1);
  private static final GenericType<JsonNode> JSON_NODE_GENERIC_TYPE =
      GenericType.of(JsonNode.class);

  @Test
  void shouldCreateMetadataForStruct() throws IOException {
    // given
    Schema schema =
        SchemaBuilder.struct()
            .name("com.example.Person")
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .build();
    Struct object = new Struct(schema).put("name", "Bobby McGee").put("age", 21);

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(object);

    // then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("age")).isEqualTo(21);
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);
  }

  @Test
  void shouldMakeMetadataForJson() throws IOException {
    // given
    String json = "{\"name\": \"Mike\"}";

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(json);

    // then
    assertThat(((TextNode) innerDataAndMetadata.getInnerData().getFieldValue("name")).textValue())
        .isEqualTo("Mike");
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
  }

  @Test
  void shouldMakeMetadataForEnclosedJson() throws IOException {
    // given
    String json = "{\"name\": {\"name2\": \"Mike\"}}";

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(json);

    // then
    assertThat(
            ((ObjectNode) innerDataAndMetadata.getInnerData().getFieldValue("name"))
                .get("name2")
                .textValue())
        .isEqualTo("Mike");
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(JSON_NODE_GENERIC_TYPE);
  }

  @Test
  void shouldTreatStringLiterallyIfItIsIncorrectJSON() throws IOException {
    // given
    String incorrectJson = "{name: Mike}";

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(incorrectJson);

    // then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo(incorrectJson);
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
  }

  @Test
  void shouldCreateMetadataFromMap() throws IOException {
    // given
    Map<String, String> fields = new HashMap<>();
    fields.put("f_1", "v_1");

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(fields);

    // then
    assertThat(((TextNode) innerDataAndMetadata.getInnerData().getFieldValue("f_1")).textValue())
        .isEqualTo("v_1");
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("f_1", CQL_TYPE))
        .isEqualTo(JSON_NODE_GENERIC_TYPE);
  }

  @Test
  void shouldCreateMetadataFromMapWithListField() throws IOException {
    // given
    Map<String, Object> fields = new HashMap<>();
    fields.put("f_1", Arrays.asList("1", "2", "3"));

    // when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(fields);

    // then
    ArrayNode f_1Value = (ArrayNode) innerDataAndMetadata.getInnerData().getFieldValue("f_1");
    assertThat(f_1Value.get(0).textValue()).isEqualTo("1");
    assertThat(f_1Value.get(1).textValue()).isEqualTo("2");
    assertThat(f_1Value.get(2).textValue()).isEqualTo("3");
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("f_1", CQL_TYPE))
        .isEqualTo(JSON_NODE_GENERIC_TYPE);
  }
}
