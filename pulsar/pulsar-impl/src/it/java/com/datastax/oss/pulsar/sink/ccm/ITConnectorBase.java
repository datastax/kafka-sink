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
package com.datastax.oss.pulsar.sink.ccm;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.sink.pulsar.CassandraSinkTask;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITConnectorBase {
  private final List<EndPoint> contactPoints;
  @Nullable private final Integer binaryPort;
  private final String localDc;
  String keyspaceName;
  protected CassandraSinkTask task = new CassandraSinkTask();
  protected SinkContext taskContext = mock(SinkContext.class);
  protected List<Map<String, Object>> taskConfigs = new ArrayList<>();
  protected final GenericSchema<GenericRecord> recordType;
  protected final GenericSchema<GenericRecord> recordTypeJson;

  public ITConnectorBase(
      List<EndPoint> contactPoints,
      @Nullable Integer binaryPort,
      String localDc,
      CqlSession session) {
    this.contactPoints = contactPoints;
    this.binaryPort = binaryPort;
    this.localDc = localDc;
    this.keyspaceName =
        session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();
    Mockito.when(taskContext.getSinkName()).thenReturn("mysink");
    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("field1").type(SchemaType.STRING);
    builder.field("double").type(SchemaType.DOUBLE);
    builder.field("float").type(SchemaType.FLOAT);
    builder.field("smallint").type(SchemaType.INT32); // INT16 is not supported in Pulsar 2.7.x
    builder.field("tinyint").type(SchemaType.INT32); // INT8 is not supported in Pulsar 2.7.x

    builder.field("bigint").type(SchemaType.INT64);
    builder.field("boolean").type(SchemaType.BOOLEAN);
    builder.field("int").type(SchemaType.INT32);
    builder.field("text").type(SchemaType.STRING);
    builder.field("my_value").type(SchemaType.STRING);
    builder.field("udtmem1").type(SchemaType.INT32);
    builder.field("udtmem2").type(SchemaType.STRING);
    builder.field("ttl").type(SchemaType.INT64);
    builder.field("timestamp").type(SchemaType.INT64);

    this.recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));
    this.recordTypeJson =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.JSON));
  }

  @AfterEach
  void stopConnector() {
    task.close();
    taskConfigs.clear();
  }

  protected void runTaskWithRecords(Record<GenericRecord>... records) {
    initConnectorAndTask();
    for (Record<GenericRecord> r : records) {
      try {
        task.write(r);
      } catch (Throwable ex) {
        // ignore
        ex.printStackTrace();
      }
    }
  }

  void initConnectorAndTask() {
    Map<String, Object> taskProps = taskConfigs.get(0);
    task.open(taskProps, taskContext);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras, String topicName) {
    Map<String, Object> props = new HashMap<>();

    props.put("name", "myinstance");
    props.put(
        "contactPoints",
        contactPoints
            .stream()
            .map(addr -> String.format("%s", ((InetSocketAddress) addr.resolve()).getHostString()))
            .collect(Collectors.joining(",")));
    if (binaryPort != null) {
      props.put("port", String.format("%d", binaryPort));
    }
    props.put("loadBalancing.localDc", localDc);
    props.put("topics", topicName);
    props.put(
        String.format("topic.%s.%s.%s.mapping", topicName, keyspaceName, tableName), mappingString);

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }

  protected Map<String, Object> makeCloudConnectorProperties(
      String mappingString,
      String tableName,
      Map<String, String> extras,
      String topicName,
      ConsistencyLevel cl) {
    Map<String, Object> props = new HashMap<>();

    props.put("name", "myinstance");

    props.put("topics", topicName);
    props.put(
        String.format("topic.%s.%s.%s.mapping", topicName, keyspaceName, tableName), mappingString);
    props.put(
        String.format("topic.%s.%s.%s.consistencyLevel", topicName, keyspaceName, tableName),
        cl.name());

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }

  protected Map<String, Object> makeConnectorProperties(Map<String, String> extras) {
    return makeConnectorProperties("bigintcol=value.bigint", extras);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, tableName, extras, "mytopic");
  }

  protected Map<String, Object> makeConnectorPropertiesWithoutContactPointsAndPort(
      String mappingString) {
    Map<String, Object> connectorProperties = makeConnectorProperties(mappingString, null);
    connectorProperties.remove("contactPoints");
    connectorProperties.remove("port");
    return connectorProperties;
  }

  protected Map<String, Object> makeConnectorProperties(String mappingString) {
    return makeConnectorProperties(mappingString, Collections.emptyMap());
  }

  public List<EndPoint> getContactPoints() {
    return contactPoints;
  }
}
