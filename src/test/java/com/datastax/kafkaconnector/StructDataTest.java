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

import java.nio.ByteBuffer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class StructDataTest {
  private final Schema schema =
      SchemaBuilder.struct()
          .name("Kafka")
          .field("bigint", Schema.INT64_SCHEMA)
          .field("boolean", Schema.BOOLEAN_SCHEMA)
          .field("bytes", Schema.BYTES_SCHEMA)
          .build();
  private final byte[] bytesArray = {3, 2, 1};
  private final Struct struct =
      new Struct(schema).put("bigint", 1234L).put("boolean", false).put("bytes", bytesArray);
  private final StructData structData = new StructData(struct);

  @Test
  void should_parse_field_names_from_struct() {
    assertThat(structData.fields()).containsExactlyInAnyOrder("bigint", "boolean", "bytes");
  }

  @Test
  void should_get_field_value() {
    assertThat(structData.getFieldValue("bigint")).isEqualTo(1234L);
    assertThat(structData.getFieldValue("boolean")).isEqualTo(false);

    // Even though the record has a byte[], we must get a ByteBuffer when we
    // retrieve it because the driver requires the input to be a ByteBuffer
    // when encoding for a blob column.
    assertThat(structData.getFieldValue("bytes")).isEqualTo(ByteBuffer.wrap(bytesArray));
  }

  @Test
  void should_handle_null_struct() {
    StructData empty = new StructData(null);
    assertThat(empty.fields()).isEmpty();
    assertThat(empty.getFieldValue("junk")).isNull();
  }
}
