package com.datastax.kafkaconnector.ccm;

import static org.mockito.Mockito.mock;

import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;

public class CCMITConnectorBase {
  private final List<EndPoint> contactPoints;
  private final int binaryPort;
  private final String localDc;
  String keyspaceName;
  DseSinkConnector conn = new DseSinkConnector();
  DseSinkTask task = new DseSinkTask();

  public CCMITConnectorBase(
      List<EndPoint> contactPoints, int binaryPort, String localDc, CqlSession session) {
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

  void runTaskWithRecords(SinkRecord... records) {
    initConnectorAndTask();
    task.put(Arrays.asList(records));
  }

  void initConnectorAndTask() {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
  }

  Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras, String topicName) {
    Map<String, String> props = new HashMap<>();

    props.put("name", "myinstance");
    props.put(
        "contactPoints",
        contactPoints
            .stream()
            .map(addr -> String.format("%s", ((InetSocketAddress) addr.resolve()).getHostString()))
            .collect(Collectors.joining(",")));
    props.put("port", String.format("%d", binaryPort));
    props.put("loadBalancing.localDc", localDc);
    props.put("topics", topicName);
    props.put(
        String.format("topic.%s.%s.%s.mapping", topicName, keyspaceName, tableName), mappingString);

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }

  Map<String, String> makeCloudConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras, String topicName) {
    Map<String, String> props = new HashMap<>();

    props.put("name", "myinstance");

    props.put("topics", topicName);
    props.put(
        String.format("topic.%s.%s.%s.mapping", topicName, keyspaceName, tableName), mappingString);

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }

  Map<String, String> makeConnectorProperties(Map<String, String> extras) {
    return makeConnectorProperties("bigintcol=value", extras);
  }

  Map<String, String> makeConnectorProperties(String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, tableName, extras, "mytopic");
  }
}
