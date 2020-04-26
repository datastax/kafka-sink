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
package com.datastax.oss.kafka.sink.ccm;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.kafka.sink.CassandraSinkConnector;
import com.datastax.oss.kafka.sink.DseSinkTask;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITConnectorBase {
  private final List<EndPoint> contactPoints;
  @Nullable private final Integer binaryPort;
  private final String localDc;
  String keyspaceName;
  protected CassandraSinkConnector conn = new CassandraSinkConnector();
  protected DseSinkTask task = new DseSinkTask();

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
    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task.initialize(taskContext);
  }

  @AfterEach
  void stopConnector() {
    task.stop();
    conn.stop();
  }

  protected void runTaskWithRecords(SinkRecord... records) {
    initConnectorAndTask();
    task.put(Arrays.asList(records));
  }

  void initConnectorAndTask() {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
  }

  protected Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras, String topicName) {
    Map<String, String> props = new HashMap<>();

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

  protected Map<String, String> makeCloudConnectorProperties(
      String mappingString,
      String tableName,
      Map<String, String> extras,
      String topicName,
      ConsistencyLevel cl) {
    Map<String, String> props = new HashMap<>();

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

  protected Map<String, String> makeConnectorProperties(Map<String, String> extras) {
    return makeConnectorProperties("bigintcol=value", extras);
  }

  protected Map<String, String> makeConnectorProperties(
      String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  protected Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, tableName, extras, "mytopic");
  }

  protected Map<String, String> makeConnectorPropertiesWithoutContactPointsAndPort(
      String mappingString) {
    Map<String, String> connectorProperties = makeConnectorProperties(mappingString, null);
    connectorProperties.remove("contactPoints");
    connectorProperties.remove("port");
    return connectorProperties;
  }

  protected Map<String, String> makeConnectorProperties(String mappingString) {
    return makeConnectorProperties(mappingString, Collections.emptyMap());
  }

  public List<EndPoint> getContactPoints() {
    return contactPoints;
  }
}
