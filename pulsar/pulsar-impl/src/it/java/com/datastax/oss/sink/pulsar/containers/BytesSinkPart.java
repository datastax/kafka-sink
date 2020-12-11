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
import static org.junit.jupiter.api.Assertions.*;

import com.datastax.driver.core.Row;
import com.datastax.oss.sink.pulsar.BytesSink;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class BytesSinkPart extends ContainersBase {

  private static Producer producer;
  static String name = "bytes";

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
        BytesSink.class);

    producer = pulsarClient.newProducer().topic(name).create();
  }

  @AfterAll
  static void destroy() throws PulsarClientException {
    producer.close();
    releaseClients();
  }

  @BeforeEach
  void before() {
    cassandraSession.execute("truncate testtbl");
  }

  @Test
  void new_topic_avro_message() throws IOException {

    waitForReadySink(name);

    producer.newMessage().value(wornBytes(statrec)).send();

    waitForProcessedMessages(name, 1);

    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(UUID.fromString((String) statrec.get("id")), r.getUUID("id"));
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));
  }
}
