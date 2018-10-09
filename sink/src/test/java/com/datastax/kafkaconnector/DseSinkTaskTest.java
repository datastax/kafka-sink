/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.record.RecordAndStatement;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DseSinkTaskTest {
  private DseSinkTask sinkTask;
  private InstanceState instanceState;
  private SinkRecord record;

  @BeforeEach
  void setUp() {
    sinkTask = new DseSinkTask();
    instanceState = mock(InstanceState.class);
    ReflectionUtils.setInternalState(sinkTask, "instanceState", instanceState);
    record = new SinkRecord("mytopic", 0, null, null, null, "value", 1234L);
  }

  @Test
  void should_map_and_queue_record() {
    // Test that if we have two mappings for one topic, we produce two bound statements.

    @SuppressWarnings("unchecked")
    BlockingQueue<RecordAndStatement> queue = new LinkedBlockingQueue<>();

    // Topic settings, using a LinkedHashMap for deterministic iteration order.
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put("topic.mytopic.ks.mytable.mapping", "c1=value");
    settings.put("topic.mytopic.ks.mytable.consistencyLevel", "ONE");
    settings.put("topic.mytopic.ks.mytable2.mapping", "c2=value");
    settings.put("topic.mytopic.ks.mytable2.consistencyLevel", "QUORUM");

    TopicConfig topicConfig = new TopicConfig("mytopic", settings);
    when(instanceState.getTopicConfig("mytopic")).thenReturn(topicConfig);
    List<TableConfig> tableConfigs = new ArrayList<>(topicConfig.getTableConfigs());
    assertThat(tableConfigs.size()).isEqualTo(2);

    RecordMapper recordMapper1 = mock(RecordMapper.class);
    RecordMapper recordMapper2 = mock(RecordMapper.class);
    when(instanceState.getRecordMapper(tableConfigs.get(0))).thenReturn(recordMapper1);
    when(instanceState.getRecordMapper(tableConfigs.get(1))).thenReturn(recordMapper2);
    BoundStatement bs1 = mock(BoundStatement.class);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(recordMapper1.map(any(), any())).thenReturn(bs1);
    when(recordMapper2.map(any(), any())).thenReturn(bs2);
    when(bs1.setConsistencyLevel(any())).thenReturn(bs1);
    when(bs2.setConsistencyLevel(any())).thenReturn(bs2);

    sinkTask.mapAndQueueRecord(queue, record);
    assertThat(queue.size()).isEqualTo(2);
    assertThat(Objects.requireNonNull(queue.poll()).getStatement()).isSameAs(bs1);
    assertThat(Objects.requireNonNull(queue.poll()).getStatement()).isSameAs(bs2);
    verify(bs1).setConsistencyLevel(DefaultConsistencyLevel.ONE);
    verify(bs2).setConsistencyLevel(DefaultConsistencyLevel.QUORUM);
  }

  @Test
  void should_categorize_statement_in_statement_group() {
    BoundStatement bs1 = mock(BoundStatement.class);
    ByteBuffer routingKey = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(bs1.getRoutingKey()).thenReturn(routingKey);

    RecordAndStatement recordAndStatement = new RecordAndStatement(record, "ks.mytable", bs1);
    Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups = new HashMap<>();

    // We don't care about the args to the constructor for this test.
    BoundStatementProcessor statementProcessor = new BoundStatementProcessor(null, null, null);
    List<RecordAndStatement> result =
        statementProcessor.categorizeStatement(statementGroups, recordAndStatement);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isSameAs(recordAndStatement);
    assertThat(statementGroups.size()).isEqualTo(1);
    assertThat(statementGroups.containsKey("ks.mytable")).isTrue();
    Map<ByteBuffer, List<RecordAndStatement>> batchGroups = statementGroups.get("ks.mytable");
    assertThat(batchGroups.size()).isEqualTo(1);
    assertThat(batchGroups.containsKey(routingKey)).isTrue();
    List<RecordAndStatement> batchGroup = batchGroups.get(routingKey);
    assertThat(batchGroup).isSameAs(result);
  }
}
