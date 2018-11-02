/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import org.apache.kafka.connect.sink.SinkRecord;

/** Simple container class to hold a SinkRecord and its associated BoundStatement. */
public class RecordAndStatement {
  private final SinkRecord record;
  private final String keyspaceAndTable;
  private final BoundStatement statement;

  public RecordAndStatement(SinkRecord record, String keyspaceAndTable, BoundStatement statement) {
    this.record = record;
    this.keyspaceAndTable = keyspaceAndTable;
    this.statement = statement;
  }

  public SinkRecord getRecord() {
    return record;
  }

  public String getKeyspaceAndTable() {
    return keyspaceAndTable;
  }

  public BoundStatement getStatement() {
    return statement;
  }
}
