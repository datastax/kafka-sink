/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.codahale.metrics.Histogram;
import com.datastax.kafkaconnector.record.RecordAndStatement;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

/**
 * Runnable class that pulls [sink-record, bound-statement] pairs from a queue and groups them based
 * on topic and routing-key, and then issues batch statements when groups are large enough
 * (currently 32). Execute BoundStatement's when there is only one in a group and we know no more
 * BoundStatements will be added to the queue.
 */
class BoundStatementProcessor implements Runnable {
  private static final int MAX_BATCH_SIZE = 32;
  private final RecordAndStatement END_STATEMENT = new RecordAndStatement(null, null, null);
  private final DseSinkTask task;
  private final BlockingQueue<RecordAndStatement> boundStatementsQueue;
  private final Collection<CompletionStage<? extends AsyncResultSet>> queryFutures;
  private final AtomicInteger successfulRecordCount = new AtomicInteger();

  BoundStatementProcessor(
      DseSinkTask task,
      BlockingQueue<RecordAndStatement> boundStatementsQueue,
      Collection<CompletionStage<? extends AsyncResultSet>> queryFutures) {
    this.task = task;
    this.boundStatementsQueue = boundStatementsQueue;
    this.queryFutures = queryFutures;
  }

  /**
   * Execute the given statements in a batch (if there is more than one statement) or individually
   * (if there is only one statement).
   *
   * @param statements list of statements to execute
   */
  private void executeStatements(List<RecordAndStatement> statements) {
    Statement statement;
    if (statements.isEmpty()) {
      // Should never happen, but just in case. No-op.
      return;
    }

    RecordAndStatement firstStatement = statements.get(0);
    InstanceState instanceState = task.getInstanceState();
    Histogram batchSizeHistogram =
        instanceState.getBatchSizeHistogram(
            firstStatement.getRecord().topic(), firstStatement.getKeyspaceAndTable());
    if (statements.size() == 1) {
      statement = firstStatement.getStatement();
      batchSizeHistogram.update(1);
    } else {
      BatchStatementBuilder bsb = BatchStatement.builder(DefaultBatchType.UNLOGGED);
      statements.stream().map(RecordAndStatement::getStatement).forEach(bsb::addStatement);
      // Construct the batch statement; set its consistency level to that of its first
      // bound statement. All bound statements in a bucket have the same CL, so this is fine.
      statement =
          bsb.build().setConsistencyLevel(firstStatement.getStatement().getConsistencyLevel());
      batchSizeHistogram.update(statements.size());
    }
    @NotNull Semaphore requestBarrier = instanceState.getRequestBarrier();
    requestBarrier.acquireUninterruptibly();
    CompletionStage<? extends AsyncResultSet> future =
        instanceState.getSession().executeAsync(statement);
    queryFutures.add(
        future.whenComplete(
            (result, ex) -> {
              requestBarrier.release();
              if (ex != null) {
                statements.forEach(
                    recordAndStatement -> {
                      SinkRecord record = recordAndStatement.getRecord();
                      task.handleFailure(
                          record,
                          ex,
                          recordAndStatement.getStatement().getPreparedStatement().getQuery(),
                          instanceState.getFailedRecordCounter());
                    });
              } else {
                successfulRecordCount.addAndGet(statements.size());
              }
              instanceState.getRecordCountMeter().mark(statements.size());
            }));
  }

  int getSuccessfulRecordCount() {
    return successfulRecordCount.get();
  }

  @Override
  public void run() {
    // Map of <topic, map<partition-key, list<recordAndStatement>>
    Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups = new HashMap<>();
    List<RecordAndStatement> pendingStatements = new ArrayList<>();
    boolean interrupted = false;
    try {
      //noinspection InfiniteLoopStatement
      while (true) {
        pendingStatements.clear();
        boundStatementsQueue.drainTo(pendingStatements);
        if (pendingStatements.isEmpty()) {
          try {
            pendingStatements.add(boundStatementsQueue.take());
          } catch (InterruptedException e) {
            interrupted = true;
            continue;
          }
        }

        for (RecordAndStatement recordAndStatement : pendingStatements) {
          if (recordAndStatement.equals(END_STATEMENT)) {
            // There are no more bound-statements being produced.
            // Create and execute remaining statement groups,
            // creating BatchStatement's when a group has more than
            // one BoundStatement.
            statementGroups
                .values()
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .forEach(this::executeStatements);
            return;
          }

          // Get the routing-key and add this statement to the appropriate
          // statement group. A statement group contains collections of
          // bound statements for a particular table. Each collection contains
          // statements for a particular routing key (a representation of partition key).

          List<RecordAndStatement> recordsAndStatements =
              categorizeStatement(statementGroups, recordAndStatement);
          if (recordsAndStatements.size() == MAX_BATCH_SIZE) {
            // We're ready to send out a batch request!
            executeStatements(recordsAndStatements);
            recordsAndStatements.clear();
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
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
  @NotNull
  List<RecordAndStatement> categorizeStatement(
      Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups,
      RecordAndStatement recordAndStatement) {
    BoundStatement statement = recordAndStatement.getStatement();
    SinkRecord sinkRecord = recordAndStatement.getRecord();
    ByteBuffer routingKey = statement.getRoutingKey();
    Map<ByteBuffer, List<RecordAndStatement>> statementGroup =
        statementGroups.computeIfAbsent(
            makeGroupKey(recordAndStatement, sinkRecord),
            t -> new HashMap<>());
    List<RecordAndStatement> recordsAndStatements =
        statementGroup.computeIfAbsent(routingKey, t -> new ArrayList<>());
    recordsAndStatements.add(recordAndStatement);
    return recordsAndStatements;
  }

  private static String makeGroupKey(RecordAndStatement recordAndStatement, SinkRecord sinkRecord) {
    return String.format("%s.%s", sinkRecord.topic(), recordAndStatement.getKeyspaceAndTable());
  }

  void stop() {
    boundStatementsQueue.add(END_STATEMENT);
  }
}
