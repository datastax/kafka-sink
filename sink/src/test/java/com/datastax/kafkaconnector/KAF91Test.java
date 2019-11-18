/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Collections;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

public class KAF91Test {

  @Test
  public void testJsonConverter() throws Exception {

    String topic = "topic";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.emptyMap(), false);

    BigDecimal expected = new BigDecimal(12.3);
    Schema schema =
        new SchemaBuilder(Schema.Type.BYTES)
            .name(Decimal.LOGICAL_NAME)
            .parameter(Decimal.SCALE_FIELD, Integer.toString(expected.scale()))
            .build();

    // Root conversion operation
    JsonNode output = objectMapper.readTree(converter.fromConnectData(topic, schema, expected));

    assertThat(output.get("payload")).isNotNull();
    assertThat(output.get("payload")).isInstanceOf(TextNode.class);
    TextNode outputText = (TextNode) output.get("payload");

    // Validate that the string in payload isn't some well-known representation of BigDecimal
    assertThatThrownBy(
            () -> {
              new BigDecimal(outputText.textValue());
            })
        .isInstanceOf(NumberFormatException.class);

    // Now validate what the text field actually is: a base64-encoded rep of the (unscaled)
    // floating point number due to the rendering as bytes (which in turn is due to the
    // ignoring of the logical type)
    BigDecimal observed =
        new BigDecimal(
            new BigInteger(Base64.getDecoder().decode(outputText.textValue())),
            Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD)));
    assertThat(expected).isEqualTo(observed);
  }
}
