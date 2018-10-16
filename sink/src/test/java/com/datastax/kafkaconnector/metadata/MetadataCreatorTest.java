package com.datastax.kafkaconnector.metadata;

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Java6Assertions.assertThat;

class MetadataCreatorTest {

  @Test
  void shouldCreateMetadataForStruct() throws IOException {
    //given
    Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build();
    Struct object = new Struct(schema)
        .put("name", "Bobby McGee")
        .put("age", 21);

    //when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(object);

    //then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("age")).isEqualTo(21);
  }

  @Test
  void shouldMakeMetadataForJson() throws IOException {
    //given
    String json = "{\"name\": \"Mike\"}";

    //when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(json);

    //then
    assertThat(((TextNode) innerDataAndMetadata.getInnerData().getFieldValue("name")).textValue()).isEqualTo("Mike");
  }

  @Test
  void shouldTreatStringLiterallyIfItIsIncorrectJSON() throws IOException {
    //given
    String incorrectJson = "{name: Mike}";

    //when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(incorrectJson);

    //then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo(incorrectJson);
  }

  @Test
  void shouldCreateMetadataFromMap() throws IOException {
    //given
    Map<String, String> fields = new HashMap<>();
    fields.put("f_1", "v_1");

    //when
    InnerDataAndMetadata innerDataAndMetadata = MetadataCreator.makeMeta(fields);

    //then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("f_1")).isEqualTo("v_1");

  }


}