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
import com.datastax.oss.sink.pulsar.GenericRecordSink;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

abstract class GenericRecordSinkPart extends ContainersBase {

  private static final Logger log = LoggerFactory.getLogger(GenericRecordSinkPart.class);

  @BeforeAll
  static void init() throws Exception {
    initClients();
  }

  @AfterAll
  static void destroy() throws PulsarClientException {
    releaseClients();
  }

  protected abstract String basicName();

  protected abstract Class<? extends GenericRecordSink> sinkClass();

  protected String name(String string) {
    return String.format("%s-%s", basicName(), string);
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
        sinkClass());
  }

  @Test
  void auto_schema_avro() throws IOException, PulsarAdminException {
    String name = name("auto-schema-avro");
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

    deleteSink(name);
  }

  @Test
  void auto_schema_json() throws IOException, PulsarAdminException {
    String name = name("auto-schema-json");
    regSink(name);

    Producer<GenericRecord> producer =
        pulsarClient.newProducer(pulsarSchema(statNode)).topic(name).create();

    waitForReadySink(name);
    GenericRecord rec = pulsarGenericJsonRecord(statNode);
    producer.newMessage().value(rec).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(statNode.get("id").textValue(), r.getUUID("id").toString());
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
    deleteSink(name);
  }

  @Test
  void predefined_schema_avro()
      throws PulsarAdminException, PulsarClientException, InterruptedException {
    String name = name("predef-schema-avro");

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schemaInfo(statrec, SchemaType.AVRO));
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
    deleteSink(name);
  }

  @Test
  void predefined_schema_json()
      throws PulsarAdminException, PulsarClientException, InterruptedException {
    String name = name("predef-schema-json");

    Schema<GenericRecord> schema = pulsarSchema(statNode);

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<GenericRecord> producer = pulsarClient.newProducer(schema).topic(name).create();

    waitForReadySink(name);
    producer.newMessage().value(pulsarGenericJsonRecord(statNode)).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(statNode.get("id").asText(), r.getUUID("id").toString());
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
    deleteSink(name);
  }

  @Test
  void various_forms_avro() throws PulsarAdminException, PulsarClientException {
    String name = name("pojo-avro");
    Schema<Pojo> schema = AvroSchema.of(Pojo.class);

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();
    waitForReadySink(name);
    Pojo pojo = new Pojo();
    pojo.setPart("pojo1");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(1969);
    pojo.setIsfact(false);

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
    deleteSink(name);
  }

  @Test
  void various_forms_json()
      throws PulsarAdminException, PulsarClientException, JsonProcessingException {
    String name = name("pojo-json");
    regSink(name);

    Producer producer = pulsarClient.newProducer().topic(name).create();
    Pojo pojo = new Pojo();
    pojo.setPart("pojo1");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(1969);
    pojo.setIsfact(false);
    Schema<Pojo> schema = JSONSchema.of(Pojo.class);

    waitForReadySink(name);
    producer.newMessage(schema).value(pojo).send();

    waitForProcessedMessages(name, 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();

    assertEquals(1, rows.size());
    Row r = rows.get(0);

    assertEquals("pojo1", r.getString("part"));
    assertEquals(UUID.fromString(pojo.getId()), r.getUUID("id"));
    assertEquals(1969, r.getInt("num"));
    assertFalse(r.getBool("fact"));

    producer.newMessage(schema).value(statNode).send();
    waitForProcessedMessages(name, 2);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P1"))).all();
    assertEquals(1, rows.size());
    r = rows.get(0);

    assertEquals("P1", r.getString("part"));
    assertEquals(statNode.get("id").textValue(), r.getUUID("id").toString());
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    ObjectNode copy = statNode.deepCopy();
    copy.put("part", "P2");
    producer.newMessage(schema).value(copy.toString()).send();
    waitForProcessedMessages(name, 3);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P2"))).all();
    assertEquals(1, rows.size());
    r = rows.get(0);

    assertEquals("P2", r.getString("part"));
    assertEquals(copy.get("id").textValue(), r.getUUID("id").toString());
    assertEquals(345, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
    deleteSink(name);
  }
}
