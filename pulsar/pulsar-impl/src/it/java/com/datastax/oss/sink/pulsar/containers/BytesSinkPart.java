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
import com.datastax.oss.sink.pulsar.BytesSink;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BytesSinkPart extends ContainersBase {

  @BeforeAll
  static void init() throws Exception {
    initClients();
  }

  private void regSink(String name) throws PulsarAdminException {
    registerSink(
        ImmutableMap.<String, Object>builder()
            .put("topics", name)
            .put("topic." + name, ConfigUtil.value(defaultSinkConfig, "topic.mytopic"))
            .build(),
        name,
        BytesSink.class);
    waitForReadySink(name);
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
  void schemaless_topic() throws IOException, PulsarAdminException {
    String name = "schlesstopic";
    regSink(name);

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();

    producer.newMessage().value(wornBytes(statrec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(statrec, rows.get(0));

    JsonNode node =
        mapper
            .createObjectNode()
            .put("part", "plainjson")
            .put("id", UUID.randomUUID().toString())
            .put("number", 1969)
            .put("isfact", false);
    producer.newMessage().value(mapper.writeValueAsBytes(node)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);

    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "plainjson"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(node, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  private void assertEqualsRec(GenericRecord rec, Row r) {
    assertEquals(rec.get("part"), r.getString("part"));
    assertEquals(UUID.fromString((String) rec.get("id")), r.getUUID("id"));
    assertEquals(rec.get("number"), r.getInt("num"));
    assertEquals(rec.get("isfact"), r.getBool("fact"));
  }

  private void assertEqualsRec(JsonNode rec, Row r) {
    assertEquals(rec.get("part").asText(), r.getString("part"));
    assertEquals(UUID.fromString(rec.get("id").asText()), r.getUUID("id"));
    assertEquals(rec.get("number").intValue(), r.getInt("num"));
    assertEquals(rec.get("isfact").booleanValue(), r.getBool("fact"));
  }

  private void assertEqualsRec(Pojo rec, Row r) {
    assertEquals(rec.getPart(), r.getString("part"));
    assertEquals(UUID.fromString(rec.getId()), r.getUUID("id"));
    assertEquals(rec.getNumber(), r.getInt("num"));
    assertEquals(rec.isIsfact(), r.getBool("fact"));
  }

  @Test
  void schemed_topic_mixed_avro_clients() throws PulsarAdminException, PulsarClientException {
    String name = "mixed-avro-clients";
    Schema<org.apache.pulsar.client.api.schema.GenericRecord> pojoAvroSchema =
        pulsarSchema(statrec);
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, pojoAvroSchema.getSchemaInfo());
    regSink(name);

    Producer<Pojo> pojoProducer =
        pulsarClient.newProducer(AvroSchema.of(Pojo.class)).topic(name).create();
    Pojo pojo = new Pojo();
    pojo.setPart("pojo");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(2021);
    pojo.setIsfact(true);
    pojoProducer.newMessage().value(pojo).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(pojo, rows.get(0));
    pojoProducer.close();

    Producer<byte[]> bytesProducer = pulsarClient.newProducer().topic(name).create();
    bytesProducer.newMessage().value(nakedBytes(statrec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P1"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(statrec, rows.get(0));
    bytesProducer.close();

    Producer<org.apache.pulsar.client.api.schema.GenericRecord> genericRecordProducer =
        pulsarClient.newProducer(pulsarSchema(statrec)).topic(name).create();
    GenericRecord rec = rec("P2", UUID.randomUUID(), 5678, false);
    genericRecordProducer.newMessage().value(pulsarGenericAvroRecord(rec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P2"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));
    genericRecordProducer.close();

    deleteSink(name);
  }

  @Test
  void schemed_topic_mixed_json_clients()
      throws PulsarAdminException, PulsarClientException, JsonProcessingException {
    String name = "mixed-json-clients";
    Schema<Pojo> pojoJsonSchema = JSONSchema.of(Pojo.class);
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, pojoJsonSchema.getSchemaInfo());
    regSink(name);

    Producer<Pojo> pojoProducer = pulsarClient.newProducer(pojoJsonSchema).topic(name).create();
    Pojo pojo = new Pojo();
    pojo.setPart("pojo");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(2021);
    pojo.setIsfact(true);
    pojoProducer.newMessage().value(pojo).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(pojo, rows.get(0));
    pojoProducer.close();

    Producer<byte[]> bytesProducer = pulsarClient.newProducer().topic(name).create();
    bytesProducer.newMessage().value(mapper.writeValueAsBytes(statNode)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P1"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(statNode, rows.get(0));
    bytesProducer.close();

    Schema<org.apache.pulsar.client.api.schema.GenericRecord> genSchema =
        Schema.generic(pojoJsonSchema.getSchemaInfo());

    Producer<org.apache.pulsar.client.api.schema.GenericRecord> genericRecordProducer =
        pulsarClient.newProducer(genSchema).topic(name).create();
    JsonNode rec =
        mapper
            .createObjectNode()
            .put("part", "P2")
            .put("id", UUID.randomUUID().toString())
            .put("number", 5678)
            .put("isfact", false);
    genericRecordProducer.newMessage().value(pulsarGenericJsonRecord(rec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    rows = cassandraSession.execute(select().from("testtbl").where(eq("part", "P2"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));
    genericRecordProducer.close();

    deleteSink(name);
  }

  @Test
  void schemed_topic_generic_avro() throws PulsarClientException, PulsarAdminException {
    String name = "generic-avro";

    Schema<org.apache.pulsar.client.api.schema.GenericRecord> schema = pulsarSchema(statrec);

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<org.apache.pulsar.client.api.schema.GenericRecord> producer =
        pulsarClient.newProducer(schema).topic(name).create();

    GenericRecord rec = rec("genericavro", UUID.randomUUID(), 9876, false);
    producer.newMessage().value(pulsarGenericAvroRecord(rec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows =
        cassandraSession.execute(select().from("testtbl").where(eq("part", "genericavro"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  @Test
  void schemed_topic_pojo_avro() throws PulsarClientException, PulsarAdminException {
    String name = "pojo-avro";

    Schema<Pojo> schema = AvroSchema.of(Pojo.class);
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<Pojo> producer = pulsarClient.newProducer(schema).topic(name).create();

    Pojo pojo = new Pojo();
    pojo.setPart("pojoavro");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(9876);
    pojo.setIsfact(false);

    producer.newMessage().value(pojo).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(pojo, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  @Test
  void schemed_topic_pojo_json() throws PulsarClientException, PulsarAdminException {
    String name = "pojo-json";

    Schema<Pojo> schema = JSONSchema.of(Pojo.class);

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<Pojo> producer = pulsarClient.newProducer(schema).topic(name).create();

    Pojo pojo = new Pojo();
    pojo.setPart("pojojson");
    pojo.setId(UUID.randomUUID().toString());
    pojo.setNumber(9876);
    pojo.setIsfact(false);

    producer.newMessage().value(pojo).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);

    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(pojo, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  @Test
  void schemed_topic_generic_json() throws PulsarClientException, PulsarAdminException {
    String name = "generic-json";
    JsonNode rec =
        mapper
            .createObjectNode()
            .put("part", "genericjson")
            .put("id", UUID.randomUUID().toString())
            .put("number", 9876)
            .put("isfact", false);

    Schema<org.apache.pulsar.client.api.schema.GenericRecord> schema = pulsarSchema(rec);

    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schema.getSchemaInfo());
    regSink(name);

    Producer<org.apache.pulsar.client.api.schema.GenericRecord> producer =
        pulsarClient.newProducer(schema).topic(name).create();

    producer.newMessage().value(pulsarGenericJsonRecord(rec)).send();
    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  @Test
  void unstructured_value() throws PulsarAdminException, PulsarClientException {
    String name = "unstructured";

    registerSink(
        ImmutableMap.<String, Object>builder()
            .put("topics", "unstructured")
            .put("topic.unstructured", ConfigUtil.value(defaultSinkConfig, "topic.mytopic"))
            .put(
                "topic.unstructured.testks.testtbl.mapping",
                "part=value,id=key,num=header.number,fact=header.isfact")
            .build(),
        name,
        BytesSink.class);
    waitForReadySink(name);

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();
    UUID id = UUID.randomUUID();
    producer
        .newMessage()
        .key(id.toString())
        .value("primval".getBytes())
        .property("number", "1969")
        .property("isfact", "true")
        .send();

    waitForProcessedMessages(name, lastMessageNum(name) + 1);
    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("primval", r.getString("part"));
    assertEquals(id, r.getUUID("id"));
    assertEquals(1969, r.getInt("num"));
    assertTrue(r.getBool("fact"));

    producer.close();
    deleteSink(name);
  }

  private ObjectMapper mapper = new ObjectMapper();

  private GenericRecord rec(String part, UUID id, int number, boolean isfact) {
    GenericRecord rec = new GenericData.Record(statrec.getSchema());
    rec.put("part", part);
    rec.put("id", id.toString());
    rec.put("number", number);
    rec.put("isfact", isfact);
    return rec;
  }

  @Test
  void schemed_topic_naked_avro() throws PulsarClientException, PulsarAdminException {

    String name = "naked-avro";
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schemaInfo(statrec, SchemaType.AVRO));
    regSink(name);

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();

    GenericRecord rec = rec("nakedavro", UUID.randomUUID(), 2021, true);
    producer.newMessage().value(nakedBytes(rec)).send();

    waitForProcessedMessages(name, lastMessageNum(name) + 1);

    List<Row> rows =
        cassandraSession.execute(select().from("testtbl").where(eq("part", "nakedavro"))).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));

    producer.close();
    deleteSink(name);
  }

  @Test
  void schemed_topic_naked_json()
      throws PulsarClientException, PulsarAdminException, JsonProcessingException {

    String name = "naked-json";
    pulsarAdmin.topics().createNonPartitionedTopic(name);
    pulsarAdmin.schemas().createSchema(name, schemaInfo(statrec, SchemaType.JSON));
    regSink(name);

    Producer<byte[]> producer = pulsarClient.newProducer().topic(name).create();

    JsonNode rec =
        mapper
            .createObjectNode()
            .put("part", "nakedjson")
            .put("id", UUID.randomUUID().toString())
            .put("number", 2021)
            .put("isfact", true);
    producer.newMessage().value(mapper.writeValueAsBytes(rec)).send();

    waitForProcessedMessages(name, lastMessageNum(name) + 1);

    List<Row> rows = cassandraSession.execute(select().from("testtbl")).all();
    assertEquals(1, rows.size());
    assertEqualsRec(rec, rows.get(0));

    producer.close();
    deleteSink(name);
  }
}
