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
package com.datastax.oss.sink.pulsar.gen;

import static java.util.Arrays.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonReader;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

public class GenSchemaTest {

  @Test
  void should_convert_complex_struct() {
    Schema schema =
        SchemaBuilder.record("myschema")
            .fields()
            .requiredString("reqstr")
            .optionalInt("optint")
            .requiredInt("reqint")
            .name("map")
            .type()
            .map()
            .values()
            .intType()
            .noDefault()
            .name("arr")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("inner")
            .type()
            .record("innerrec")
            .fields()
            .optionalInt("inneroptint")
            .requiredString("innerreqstr")
            .name("innermap")
            .type()
            .map()
            .values()
            .stringType()
            .noDefault()
            .endRecord()
            .noDefault()
            .name("nestedmap")
            .type()
            .map()
            .values()
            .map()
            .values()
            .intType()
            .noDefault()
            .endRecord();

    GenericRecord rec = new GenericData.Record(schema);
    rec.put("reqstr", "the string");
    rec.put("optint", 6);
    rec.put("reqint", 8);
    rec.put("map", ImmutableMap.builder().put("k1", 1).put("k2", 2).put("k3", 3).build());
    GenericRecord inner = new GenericData.Record(schema.getField("inner").schema());
    inner.put("innerreqstr", "inner string");
    inner.put(
        "innermap",
        ImmutableMap.builder().put("ik1", "iv1").put("ik2", "iv2").put("ik3", "iv3").build());
    rec.put("inner", inner);
    rec.put(
        "arr", new GenericData.Array<>(schema.getField("arr").schema(), asList("hello", "world")));
    rec.put(
        "nestedmap",
        ImmutableMap.builder()
            .put("nmk1", ImmutableMap.builder().put("nmk11", 5).put("nmk12", 4).build())
            .put("nmk2", ImmutableMap.builder().put("nmk22", 7).build())
            .build());

    SchemaInfo info =
        new SchemaInfo(
            schema.getName(),
            schema.toString().getBytes(),
            SchemaType.AVRO,
            Collections.emptyMap());
    GenericSchema<org.apache.pulsar.client.api.schema.GenericRecord> avroSchema =
        DefaultImplementation.getGenericSchema(info);
    org.apache.pulsar.client.api.schema.GenericRecord record =
        new GenericAvroRecord(null, schema, avroSchema.getFields(), rec);

    GenStruct struct = GenSchema.convert(record);

    GenSchema.StructGenSchema structSchema = struct.getSchema();

    assertEquals(7, struct.fields().size());
    assertEquals(7, structSchema.fieldNames().size());
    assertTrue(
        struct.fields().containsAll(asList("reqstr", "optint", "reqint", "map", "arr", "inner")));
    assertTrue(
        structSchema
            .fieldNames()
            .containsAll(asList("reqstr", "optint", "reqint", "map", "arr", "inner")));

    assertSame(GenSchema.STRING, structSchema.field("reqstr"));
    assertEquals("the string", struct.value("reqstr"));
    assertSame(GenSchema.INT32, structSchema.field("optint"));
    assertEquals(6, struct.value("optint"));
    assertSame(GenSchema.INT32, structSchema.field("reqint"));
    assertEquals(8, struct.value("reqint"));

    assertTrue(structSchema.field("map") instanceof GenSchema.MapGenSchema);
    assertSame(
        GenSchema.INT32, ((GenSchema.MapGenSchema) structSchema.field("map")).elementSchema());
    assertTrue(struct.value("map") instanceof Map);
    Map<String, ?> mapval = (Map<String, ?>) struct.value("map");
    assertEquals(3, mapval.size());
    assertTrue(mapval.keySet().containsAll(asList("k1", "k2", "k3")));
    assertEquals(2, mapval.get("k2"));

    assertTrue(structSchema.field("arr") instanceof GenSchema.ArrayGenSchema);
    assertSame(
        GenSchema.STRING, ((GenSchema.ArrayGenSchema) structSchema.field("arr")).elementSchema());
    assertTrue(struct.value("arr") instanceof List);
    List<?> listval = (List<?>) struct.value("arr");
    assertEquals(2, listval.size());
    assertEquals("hello", listval.get(0));
    assertEquals("world", listval.get(1));

    assertTrue(structSchema.field("inner") instanceof GenSchema.StructGenSchema);
    GenSchema.StructGenSchema innerStructSchema =
        (GenSchema.StructGenSchema) structSchema.field("inner");
    assertTrue(struct.value("inner") instanceof GenStruct);
    GenStruct structval = (GenStruct) struct.value("inner");
    assertEquals(3, structval.fields().size());
    assertTrue(structval.fields().containsAll(asList("inneroptint", "innerreqstr", "innermap")));
    assertSame(GenSchema.STRING, innerStructSchema.field("inneroptint"));
    assertNull(structval.value("inneroptint"));
    assertSame(GenSchema.STRING, innerStructSchema.field("innerreqstr"));
    assertEquals("inner string", structval.value("innerreqstr"));

    assertTrue(innerStructSchema.field("innermap") instanceof GenSchema.MapGenSchema);
    GenSchema.MapGenSchema innerMapSchema =
        (GenSchema.MapGenSchema) innerStructSchema.field("innermap");
    assertTrue(structval.value("innermap") instanceof Map);
    Map<String, ?> imapval = (Map<String, ?>) structval.value("innermap");
    assertEquals(3, imapval.size());
    assertTrue(imapval.keySet().containsAll(asList("ik1", "ik2", "ik3")));
    assertSame(GenSchema.STRING, innerMapSchema.elementSchema());
    assertEquals("iv3", imapval.get("ik3"));
  }

  private ObjectMapper mapper = new ObjectMapper();

  @Test
  void json_with_null() throws JsonProcessingException {
    JsonNode node = mapper.createObjectNode().put("number", 12345).putNull("text");
    byte[] json = "{\"number\":12345, \"text\":null}".getBytes();
    GenericJsonRecord rec =
        new GenericJsonReader(Arrays.asList(new Field("number", 1), new Field("text", 2)))
            .read(json, 0, json.length);
    GenStruct struct = GenSchema.convert(rec);
    //    assertEquals(12345, struct.value("number"));
    assertTrue(rec.getJsonNode().get("text").isNull());
    //    assertNull(struct.value("text"));

  }

  @Test
  void anotherAssert() {
    byte[] json = "{\"somefield\":null}".getBytes();
    GenericJsonRecord record =
        new GenericJsonReader(Collections.singletonList(new Field("somefield", 0)))
            .read(json, 0, json.length);
    assert record.getJsonNode().get("somefield").isNull();
    assert "null".equals(record.getField("somefield"));
    assert record.getField("somefield") != null;
  }
}
