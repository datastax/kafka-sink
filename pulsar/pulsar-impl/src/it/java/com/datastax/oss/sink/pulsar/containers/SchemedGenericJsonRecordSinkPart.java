/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar.containers;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.datastax.driver.core.Row;
import com.datastax.oss.sink.pulsar.SchemedGenericRecordSink;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class SchemedGenericJsonRecordSinkPart extends ContainersBase {

  private static String name = "schemed-json";

  @SuppressWarnings("unchecked")
  @BeforeAll
  static void init() throws Exception {
    initClients();

    registerSink(
        ImmutableMap.<String, Object>builder()
            .put("topics", name)
            .put("topic." + name, ConfigUtil.value(defaultSinkConfig, "topic.mytopic"))
            .build(),
        name,
        SchemedGenericRecordSink.class);
  }

  @AfterAll
  static void destroy() throws PulsarClientException {
    releaseClients();
  }

  @BeforeEach
  void before() {
    cassandraSession.execute("truncate testtbl");
  }

  @Test
  void jsonRecord() throws IOException {

    UUID id = UUID.randomUUID();

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node =
        mapper
            .createObjectNode()
            .put("part", "P1")
            .put("id", id.toString())
            .put("number", 345)
            .put("isfact", true);

    Producer<GenericRecord> producer =
        pulsarClient.newProducer(pulsarSchema(node)).topic(name).create();

    waitForReadySink(name);
    GenericRecord rec = pulsarGenericJsonRecord(node);
    producer.newMessage().value(rec).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(id, r.getUUID("id"));
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
  }
}
