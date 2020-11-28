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
package com.datastax.oss.sink.pulsar.ccm;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITConnectorBase<Coat> {
  private final List<EndPoint> contactPoints;
  @Nullable private final Integer binaryPort;
  private final String localDc;
  String keyspaceName;
  protected Sink<Coat> conn;

  public ITConnectorBase(
      List<EndPoint> contactPoints,
      @Nullable Integer binaryPort,
      String localDc,
      CqlSession session,
      Sink<Coat> sink) {
    this.contactPoints = contactPoints;
    this.binaryPort = binaryPort;
    this.localDc = localDc;
    this.keyspaceName =
        session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();
    this.conn = sink;
  }

  @AfterEach
  void stopConnector() throws Exception {
    conn.close();
  }

  protected void runTaskWithRecords(Map<String, Object> config, Record<Coat> record)
      throws Exception {
    initConnectorAndTask(config);
    runTaskWithRecords(record);
  }

  protected void runTaskWithRecords(Record<Coat> record) throws Exception {
    conn.write(record);
  }

  void initConnectorAndTask(Map<String, Object> config) throws Exception {
    conn.open(config, null);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, String tableName, Map<String, Object> extras, String topicName) {
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

  protected Map<String, Object> makeConnectorProperties(Map<String, Object> extras) {
    return makeConnectorProperties("bigintcol=value", extras);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, Map<String, Object> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  protected Map<String, Object> makeConnectorProperties(
      String mappingString, String tableName, Map<String, Object> extras) {
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
