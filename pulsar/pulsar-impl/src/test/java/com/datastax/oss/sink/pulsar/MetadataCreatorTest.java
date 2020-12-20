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
package com.datastax.oss.sink.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.metadata.InnerDataAndMetadata;
import com.datastax.oss.common.sink.metadata.MetadataCreator;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class MetadataCreatorTest {

  private static final PrimitiveType CQL_TYPE = new PrimitiveType(-1);
  private static final GenericType<JsonNode> JSON_NODE_GENERIC_TYPE =
      GenericType.of(JsonNode.class);

  public static class MyPojo {

    String name;
    int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

  @Test
  void shouldCreateMetadataForStruct() throws IOException {
    // given
    Schema schema = Schema.JSON(MyPojo.class);
    GenericRecord object = new GenericRecordImpl().put("name", "Bobby McGee").put("age", 21);
    Record<GenericRecord> record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(
            record, localSchemaRegistry.ensureAndUpdateSchema(record), localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadata =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    // then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("age")).isEqualTo(21);
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);
  }
}
