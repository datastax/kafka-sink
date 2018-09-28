/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import org.apache.kafka.connect.sink.SinkRecord;

/** Simple container class to hold a SinkRecord and its associated BoundStatement. */
class RecordAndStatement {
  private final SinkRecord record;
  private final String keyspaceAndTable;
  private final BoundStatement statement;

  RecordAndStatement(SinkRecord record, String keyspaceAndTable, BoundStatement statement) {
    this.record = record;
    this.keyspaceAndTable = keyspaceAndTable;
    this.statement = statement;
  }

  SinkRecord getRecord() {
    return record;
  }

  String getKeyspaceAndTable() {
    return keyspaceAndTable;
  }

  BoundStatement getStatement() {
    return statement;
  }
}
