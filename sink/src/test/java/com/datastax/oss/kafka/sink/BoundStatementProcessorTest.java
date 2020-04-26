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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.kafka.sink.record.RecordAndStatement;
import com.datastax.oss.kafka.sink.state.InstanceState;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BoundStatementProcessorTest {

  @Test
  void should_categorize_statement_in_statement_group() {

    BoundStatement bs1 = mock(BoundStatement.class);
    ByteBuffer routingKey = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(bs1.getRoutingKey()).thenReturn(routingKey);

    SinkRecord record1 = new SinkRecord("mytopic", 0, null, null, null, "value", 1234L);
    RecordAndStatement recordAndStatement1 = new RecordAndStatement(record1, "ks.mytable", bs1);

    SinkRecord record2 = new SinkRecord("yourtopic", 0, null, null, null, "value", 1234L);
    RecordAndStatement recordAndStatement2 = new RecordAndStatement(record2, "ks.mytable", bs1);

    Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups = new HashMap<>();

    // We don't care about the args to the constructor for this test.
    BoundStatementProcessor statementProcessor =
        new BoundStatementProcessor(mockDseSinkTask(), null, null, 32);

    // Categorize the two statements. Although they refer to the same ks/table and have the
    // same routing key, they should be in different buckets.
    List<RecordAndStatement> result1 =
        statementProcessor.categorizeStatement(statementGroups, recordAndStatement1);
    List<RecordAndStatement> result2 =
        statementProcessor.categorizeStatement(statementGroups, recordAndStatement2);

    assertThat(result1.size()).isEqualTo(1);
    assertThat(result1.get(0)).isSameAs(recordAndStatement1);
    assertThat(statementGroups.size()).isEqualTo(2);
    assertThat(statementGroups.containsKey("mytopic.ks.mytable")).isTrue();
    Map<ByteBuffer, List<RecordAndStatement>> batchGroups =
        statementGroups.get("mytopic.ks.mytable");
    assertThat(batchGroups.size()).isEqualTo(1);
    assertThat(batchGroups.containsKey(routingKey)).isTrue();
    List<RecordAndStatement> batchGroup = batchGroups.get(routingKey);
    assertThat(batchGroup).isSameAs(result1);

    batchGroups = statementGroups.get("yourtopic.ks.mytable");
    assertThat(batchGroups.size()).isEqualTo(1);
    assertThat(batchGroups.containsKey(routingKey)).isTrue();
    batchGroup = batchGroups.get(routingKey);
    assertThat(batchGroup).isSameAs(result2);
  }

  @ParameterizedTest(
    name =
        "[{index}] totalNumberOfRecords={0}, maxNumberOfRecordsInBatch={1}, expectedBatchSizes={2}"
  )
  @MethodSource("batchSizes")
  void should_create_batches_of_expected_size(
      int totalNumberOfRecords, int maxNumberOfRecordsInBatch, int[] expectedBatchSizes)
      throws InterruptedException {
    // given
    BlockingQueue<RecordAndStatement> recordAndStatements = new LinkedBlockingQueue<>();
    BoundStatementProcessor statementProcessor =
        new BoundStatementProcessor(
            mockDseSinkTask(), recordAndStatements, new ArrayList<>(), maxNumberOfRecordsInBatch);
    List<List<RecordAndStatement>> actualBatches = new ArrayList<>();
    // we need to copy the batch into a new list since the original one may be cleared after
    Consumer<List<RecordAndStatement>> mockConsumer = e -> actualBatches.add(new ArrayList<>(e));
    ByteBuffer routingKey = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

    // when
    // emulate DseSinkTask.put() behavior
    Thread producer =
        new Thread(
            () -> {
              for (int i = 0; i < totalNumberOfRecords; i++) {
                SinkRecord record = new SinkRecord("mytopic", i, null, null, null, i, i);
                BoundStatement statement = mock(BoundStatement.class);
                when(statement.getRoutingKey()).thenReturn(routingKey);
                recordAndStatements.add(new RecordAndStatement(record, "ks.tb", statement));
              }
              statementProcessor.stop();
            });

    // emulate BoundStatementProcessor.run() behavior
    Thread consumer =
        new Thread(
            () -> {
              try {
                statementProcessor.runLoop(mockConsumer);
              } catch (InterruptedException ignored) {
              }
            });

    producer.start();
    consumer.start();
    producer.join();
    consumer.join();

    // then
    assertThat(actualBatches).hasSize(expectedBatchSizes.length);
    for (int i = 0; i < actualBatches.size(); i++) {
      assertThat(actualBatches.get(i)).hasSize(expectedBatchSizes[i]);
    }
  }

  @Test
  void should_group_batch_by_a_partition_key_not_an_input_topic_key() throws InterruptedException {
    // given
    BlockingQueue<RecordAndStatement> recordAndStatements = new LinkedBlockingQueue<>();
    BoundStatementProcessor statementProcessor =
        new BoundStatementProcessor(mockDseSinkTask(), recordAndStatements, new ArrayList<>(), 3);
    List<List<RecordAndStatement>> actualBatches = new ArrayList<>();
    // we need to copy the batch into a new list since the original one may be cleared after
    Consumer<List<RecordAndStatement>> mockConsumer = e -> actualBatches.add(new ArrayList<>(e));

    // when
    // emulate DseSinkTask.put() behavior
    Thread producer =
        new Thread(
            () -> {
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  1,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  2,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  3,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {2}));

              statementProcessor.stop();
            });

    // emulate BoundStatementProcessor.run() behavior
    Thread consumer =
        new Thread(
            () -> {
              try {
                statementProcessor.runLoop(mockConsumer);
              } catch (InterruptedException ignored) {
              }
            });

    producer.start();
    consumer.start();
    producer.join();
    consumer.join();

    // then
    assertThat(actualBatches.size()).isEqualTo(2);
    assertThat(actualBatches.get(0).size()).isEqualTo(2);
    assertThat(actualBatches.get(1).size()).isEqualTo(1);
  }

  @Test
  void should_create_two_batches_for_the_same_dse_tables_but_different_input_topics()
      throws InterruptedException {
    // given
    BlockingQueue<RecordAndStatement> recordAndStatements = new LinkedBlockingQueue<>();
    BoundStatementProcessor statementProcessor =
        new BoundStatementProcessor(mockDseSinkTask(), recordAndStatements, new ArrayList<>(), 3);
    List<List<RecordAndStatement>> actualBatches = new ArrayList<>();
    // we need to copy the batch into a new list since the original one may be cleared after
    Consumer<List<RecordAndStatement>> mockConsumer = e -> actualBatches.add(new ArrayList<>(e));

    // when
    // emulate DseSinkTask.put() behavior
    Thread producer =
        new Thread(
            () -> {
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  1,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  2,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));
              addSinkRecord(
                  recordAndStatements,
                  "topic-different",
                  "keyspace1",
                  "table1",
                  2,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));

              statementProcessor.stop();
            });

    // emulate BoundStatementProcessor.run() behavior
    Thread consumer =
        new Thread(
            () -> {
              try {
                statementProcessor.runLoop(mockConsumer);
              } catch (InterruptedException ignored) {
              }
            });

    producer.start();
    consumer.start();
    producer.join();
    consumer.join();

    // then
    assertThat(actualBatches.size()).isEqualTo(2);
    assertThat(actualBatches.get(0).size()).isEqualTo(2);
    assertThat(actualBatches.get(1).size()).isEqualTo(1);
  }

  @Test
  void should_create_two_batches_for_different_dse_tables_and_same_partition_key()
      throws InterruptedException {
    // given
    BlockingQueue<RecordAndStatement> recordAndStatements = new LinkedBlockingQueue<>();
    BoundStatementProcessor statementProcessor =
        new BoundStatementProcessor(mockDseSinkTask(), recordAndStatements, new ArrayList<>(), 2);
    List<List<RecordAndStatement>> actualBatches = new ArrayList<>();
    // we need to copy the batch into a new list since the original one may be cleared after
    Consumer<List<RecordAndStatement>> mockConsumer = e -> actualBatches.add(new ArrayList<>(e));

    // when
    // emulate DseSinkTask.put() behavior
    Thread producer =
        new Thread(
            () -> {
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table1",
                  1,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));
              addSinkRecord(
                  recordAndStatements,
                  "topic1",
                  "keyspace1",
                  "table-different",
                  2,
                  "value_1",
                  ByteBuffer.wrap(new byte[] {1}));

              statementProcessor.stop();
            });

    // emulate BoundStatementProcessor.run() behavior
    Thread consumer =
        new Thread(
            () -> {
              try {
                statementProcessor.runLoop(mockConsumer);
              } catch (InterruptedException ignored) {
              }
            });

    producer.start();
    consumer.start();
    producer.join();
    consumer.join();

    // then
    assertThat(actualBatches.size()).isEqualTo(2);
    assertThat(actualBatches.get(0).size()).isEqualTo(1);
    assertThat(actualBatches.get(1).size()).isEqualTo(1);
  }

  private void addSinkRecord(
      BlockingQueue<RecordAndStatement> recordAndStatements,
      String topic,
      String keyspace,
      String table,
      Object kafkaKey,
      Object kafkaValue,
      ByteBuffer dseRoutingKey) {
    SinkRecord record = new SinkRecord(topic, 1, null, kafkaKey, null, kafkaValue, 1234);
    BoundStatement statement = mock(BoundStatement.class);
    when(statement.getRoutingKey()).thenReturn(dseRoutingKey);
    recordAndStatements.add(new RecordAndStatement(record, keyspace + "." + table, statement));
  }

  private static Stream<? extends Arguments> batchSizes() {
    return Stream.of(
        Arguments.of(1, 1, new int[] {1}),
        Arguments.of(10, 10, new int[] {10}),
        Arguments.of(10, 5, new int[] {5, 5}),
        Arguments.of(9, 5, new int[] {5, 4}),
        Arguments.of(11, 5, new int[] {5, 5, 1}),
        Arguments.of(0, 1, new int[] {}));
  }

  private DseSinkTask mockDseSinkTask() {
    InstanceState instanceState = mock(InstanceState.class);
    when(instanceState.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
    when(instanceState.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    DseSinkTask sinkTask = mock(DseSinkTask.class);
    when(sinkTask.getInstanceState()).thenReturn(instanceState);
    return sinkTask;
  }
}
