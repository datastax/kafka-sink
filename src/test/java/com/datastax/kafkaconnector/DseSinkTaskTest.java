/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DseSinkTaskTest {
  private DseSinkTask sinkTask;
  private SinkRecord record;

  DseSinkTaskTest() {
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("f1", Schema.INT64_SCHEMA)
            .field("f2", Schema.BOOLEAN_SCHEMA)
            .field("f3", Schema.FLOAT64_SCHEMA);
    Struct value = new Struct(schema).put("f1", 1234L).put("f2", true).put("f3", 42.5);
    record = new SinkRecord("mytopic", 0, null, null, schema, value, 9876L);
  }

  @BeforeEach
  void setUp() {
    sinkTask = new DseSinkTask();
  }

  @Test
  void should_error_if_record_is_missing_field() {
    assertThatThrownBy(() -> sinkTask.getFieldValue(record, "value.noexist"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("noexist is not a valid field name");
  }
}
