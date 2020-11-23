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
package com.datastax.oss.sink;

import com.codahale.metrics.Histogram;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.sampler.DataSizes;
import com.datastax.oss.sink.record.RecordAndStatement;
import com.datastax.oss.sink.state.InstanceState;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Runnable class that pulls [sink-record, bound-statement] pairs from a queue and groups them based
 * on topic and routing-key, and then issues batch statements when groups are large enough
 * (currently 32). Execute BoundStatement's when there is only one in a group and we know no more
 * BoundStatements will be added to the queue.
 */
public class BoundStatementProcessor<EngineRecord> implements Callable<Void> {
  private static final RecordAndStatement END_STATEMENT =
      new RecordAndStatement<>(null, null, null);
  private final RecordProcessor<EngineRecord, ?> task;
  private final BlockingQueue<RecordAndStatement<EngineRecord>> boundStatementsQueue;
  private final Collection<CompletionStage<? extends AsyncResultSet>> queryFutures;
  private final int maxNumberOfRecordsInBatch;
  private final AtomicInteger successfulRecordCount = new AtomicInteger();
  private final ProtocolVersion protocolVersion;
  private final CodecRegistry codecRegistry;

  public BoundStatementProcessor(
      RecordProcessor<EngineRecord, ?> task,
      BlockingQueue<RecordAndStatement<EngineRecord>> boundStatementsQueue,
      Collection<CompletionStage<? extends AsyncResultSet>> queryFutures,
      int maxNumberOfRecordsInBatch) {
    this.task = task;
    this.boundStatementsQueue = boundStatementsQueue;
    this.queryFutures = queryFutures;
    this.maxNumberOfRecordsInBatch = maxNumberOfRecordsInBatch;
    this.protocolVersion = task.getInstanceState().getProtocolVersion();
    this.codecRegistry = task.getInstanceState().getCodecRegistry();
  }

  /**
   * Execute the given statements in a batch (if there is more than one statement) or individually
   * (if there is only one statement).
   *
   * @param statements list of statements to execute
   */
  private void executeStatements(List<RecordAndStatement<EngineRecord>> statements) {
    Statement statement;
    if (statements.isEmpty()) {
      // Should never happen, but just in case. No-op.
      return;
    }

    RecordAndStatement<EngineRecord> firstStatement = statements.get(0);
    InstanceState instanceState = task.getInstanceState();
    Histogram batchSizeHistogram =
        instanceState.getBatchSizeHistogram(
            task.apiAdapter().topic(firstStatement.getRecord()),
            firstStatement.getKeyspaceAndTable());
    Histogram batchSizeInBytesHistogram =
        instanceState.getBatchSizeInBytesHistogram(
            task.apiAdapter().topic(firstStatement.getRecord()),
            firstStatement.getKeyspaceAndTable());

    Consumer<Integer> recordIncrement =
        v ->
            instanceState.incrementRecordCounter(
                task.apiAdapter().topic(firstStatement.getRecord()),
                firstStatement.getKeyspaceAndTable(),
                v);
    Runnable failedRecordIncrement =
        () ->
            instanceState.incrementFailedCounter(
                task.apiAdapter().topic(firstStatement.getRecord()),
                firstStatement.getKeyspaceAndTable());

    if (statements.size() == 1) {
      statement = firstStatement.getStatement();
      updateBatchSizeMetrics(statement, batchSizeHistogram, batchSizeInBytesHistogram);
    } else {
      BatchStatementBuilder bsb = BatchStatement.builder(DefaultBatchType.UNLOGGED);
      statements.stream().map(RecordAndStatement::getStatement).forEach(bsb::addStatement);
      // Construct the batch statement; set its consistency level to that of its first
      // bound statement. All bound statements in a bucket have the same CL, so this is fine.
      statement =
          bsb.build().setConsistencyLevel(firstStatement.getStatement().getConsistencyLevel());
      updateBatchSizeMetrics(statements, batchSizeHistogram, batchSizeInBytesHistogram);
    }
    @NonNull Semaphore requestBarrier = instanceState.getRequestBarrier();
    requestBarrier.acquireUninterruptibly();
    CompletionStage<? extends AsyncResultSet> future =
        instanceState.getSession().executeAsync(statement);
    queryFutures.add(
        future.whenComplete(
            (result, ex) -> {
              requestBarrier.release();
              if (ex != null) {
                statements.forEach(
                    recordAndStatement ->
                        task.handleFailure(
                            recordAndStatement.getRecord(),
                            ex,
                            recordAndStatement.getStatement().getPreparedStatement().getQuery(),
                            failedRecordIncrement));
              } else {
                successfulRecordCount.addAndGet(statements.size());
                statements.stream().map(RecordAndStatement::getRecord).forEach(task::handleSuccess);
              }
              recordIncrement.accept(statements.size());
            }));
  }

  private void updateBatchSizeMetrics(
      List<RecordAndStatement<EngineRecord>> statements,
      Histogram batchSizeHistogram,
      Histogram batchSizeInBytesHistogram) {
    statements.forEach(
        s ->
            batchSizeInBytesHistogram.update(
                DataSizes.getDataSize(s.getStatement(), protocolVersion, codecRegistry)));
    batchSizeHistogram.update(statements.size());
  }

  private void updateBatchSizeMetrics(
      Statement<?> statement, Histogram batchSizeHistogram, Histogram batchSizeInBytesHistogram) {
    batchSizeInBytesHistogram.update(
        DataSizes.getDataSize(statement, protocolVersion, codecRegistry));
    batchSizeHistogram.update(1);
  }

  int getSuccessfulRecordCount() {
    return successfulRecordCount.get();
  }

  @Override
  public Void call() throws InterruptedException {
    runLoop(this::executeStatements);
    return null;
  }

  @VisibleForTesting
  public void runLoop(Consumer<List<RecordAndStatement<EngineRecord>>> consumer)
      throws InterruptedException {
    // Map of <topic, map<partition-key, list<recordAndStatement>>
    Map<String, Map<ByteBuffer, List<RecordAndStatement<EngineRecord>>>> statementGroups =
        new HashMap<>();
    while (true) {

      // Note: this call may block indefinitely if stop() is never called.
      // It is the producer's responsibility to call stop() when there are no more records
      // to process.
      RecordAndStatement<EngineRecord> recordAndStatement = boundStatementsQueue.take();

      if (recordAndStatement == END_STATEMENT) {
        // There are no more bound-statements being produced.
        // Create and execute remaining statement groups,
        // creating BatchStatement's when a group has more than
        // one BoundStatement.
        statementGroups
            .values()
            .stream()
            .map(Map::values)
            .flatMap(Collection::stream)
            .filter(recordAndStatements -> !recordAndStatements.isEmpty())
            .map(ImmutableList::copyOf)
            .forEach(consumer);
        return;
      }

      // Get the routing-key and add this statement to the appropriate
      // statement group. A statement group contains collections of
      // bound statements for a particular table. Each collection contains
      // statements for a particular routing key (a representation of partition key).

      List<RecordAndStatement<EngineRecord>> recordsAndStatements =
          categorizeStatement(statementGroups, recordAndStatement);
      if (recordsAndStatements.size() == maxNumberOfRecordsInBatch) {
        // We're ready to send out a batch request!
        consumer.accept(ImmutableList.copyOf(recordsAndStatements));
        recordsAndStatements.clear();
      }
    }
  }

  /**
   * Categorize the given statement into the appropriate statement group, based on keyspace/table
   * and routing key.
   *
   * @param statementGroups running collection of categorized statements that are pending execution
   * @param recordAndStatement the record/statement that needs to be put in a bucket
   * @return The specific bucket (list) to which the record/statement was added.
   */
  @VisibleForTesting
  @NonNull
  public List<RecordAndStatement<EngineRecord>> categorizeStatement(
      Map<String, Map<ByteBuffer, List<RecordAndStatement<EngineRecord>>>> statementGroups,
      RecordAndStatement<EngineRecord> recordAndStatement) {
    BoundStatement statement = recordAndStatement.getStatement();
    EngineRecord sinkRecord = recordAndStatement.getRecord();
    ByteBuffer routingKey = statement.getRoutingKey();
    Map<ByteBuffer, List<RecordAndStatement<EngineRecord>>> statementGroup =
        statementGroups.computeIfAbsent(
            makeGroupKey(recordAndStatement, sinkRecord), t -> new HashMap<>());
    List<RecordAndStatement<EngineRecord>> recordsAndStatements =
        statementGroup.computeIfAbsent(routingKey, t -> new ArrayList<>());
    recordsAndStatements.add(recordAndStatement);
    return recordsAndStatements;
  }

  private String makeGroupKey(
      RecordAndStatement<EngineRecord> recordAndStatement, EngineRecord sinkRecord) {
    return String.format(
        "%s.%s", task.apiAdapter().topic(sinkRecord), recordAndStatement.getKeyspaceAndTable());
  }

  @SuppressWarnings("unchecked")
  public void stop() {
    boundStatementsQueue.add(END_STATEMENT);
  }
}
