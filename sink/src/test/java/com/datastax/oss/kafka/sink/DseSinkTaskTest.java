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
package com.datastax.oss.kafka.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.kafka.sink.config.TableConfig;
import com.datastax.oss.kafka.sink.config.TopicConfig;
import com.datastax.oss.kafka.sink.record.RecordAndStatement;
import com.datastax.oss.kafka.sink.state.InstanceState;
import java.util.ArrayList;
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

    TopicConfig topicConfig = new TopicConfig("mytopic", settings, false);
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
}
