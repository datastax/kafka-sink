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
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class SchemedGenericAvroRecordSinkPart extends ContainersBase {

  private static final Logger log = LoggerFactory.getLogger(SchemedGenericAvroRecordSinkPart.class);

  @BeforeAll
  static void init() throws Exception {
    initClients();
  }

  @AfterAll
  static void destroy() throws PulsarClientException {
    releaseClients();
  }

  @BeforeEach
  void before() {
    cassandraSession.execute("truncate testtbl");
  }

  private void regSink(String name) throws PulsarAdminException {
    registerSink(
        ImmutableMap.<String, Object>builder()
            .put("topics", name)
            .put("topic." + name, ConfigUtil.value(defaultSinkConfig, "topic.mytopic"))
            .build(),
        name,
        SchemedGenericRecordSink.class);
  }

  @Test
  void new_topic_with_no_schema() throws IOException, PulsarAdminException {
    String name = "schemed-avro";
    regSink(name);
    Producer<GenericRecord> producer =
        pulsarClient.newProducer(pulsarSchema(statrec)).topic(name).create();
    waitForReadySink(name);
    producer.newMessage().value(pulsarGenericAvroRecord(statrec)).send();
    waitForProcessedMessages(name, 1);

    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("P1", r.getString("part"));
    assertEquals(UUID.fromString(statrec.get("id").toString()), r.getUUID("id"));
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
  }

  @Test
  void existent_topic_with_schema()
      throws PulsarAdminException, PulsarClientException, InterruptedException {
    String name = "exst-with-schema";

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, recSchemaInfo(SchemaType.AVRO));
    regSink(name);

    Producer<GenericRecord> producer =
        pulsarClient.newProducer(pulsarSchema(statrec)).topic(name).create();

    waitForReadySink(name);
    producer.newMessage().value(pulsarGenericAvroRecord(statrec)).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(UUID.fromString(statrec.get("id").toString()), r.getUUID("id"));
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
  }

  @Test
  void pojo_and_generic() throws PulsarAdminException, PulsarClientException {
    String name = "pojo";
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, recSchemaInfo(SchemaType.AVRO));
    regSink(name);

    Producer producer = pulsarClient.newProducer().topic(name).create();
    waitForReadySink(name);
    Pojo pojo = new Pojo();
    pojo.setPart("pojo1");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(1969);
    pojo.setIsfact(false);
    Schema<Pojo> schema = AvroSchema.of(Pojo.class);

    producer.newMessage(schema).value(pojo).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("pojo1", r.getString("part"));
    assertEquals(UUID.fromString(pojo.getId()), r.getUUID("id"));
    assertEquals(1969, r.getInt("num"));
    assertFalse(r.getBool("fact"));

    producer.newMessage(pulsarSchema(statrec)).value(pulsarGenericAvroRecord(statrec)).send();
    waitForProcessedMessages(name, 2);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P1"))).all();
    assertEquals(1, rows.size());
    r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(UUID.fromString(statrec.get("id").toString()), r.getUUID("id"));
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
  }

  public static class Pojo {
    private String part;
    private String id;
    private int number;
    private boolean isfact;

    public String getPart() {
      return part;
    }

    public void setPart(String part) {
      this.part = part;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getNumber() {
      return number;
    }

    public void setNumber(int number) {
      this.number = number;
    }

    public boolean isIsfact() {
      return isfact;
    }

    public void setIsfact(boolean isfact) {
      this.isfact = isfact;
    }
  }
}
