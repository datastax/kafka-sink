/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

class SessionState {
  private final DseSession session;
  private final KafkaCodecRegistry codecRegistry;
  private final PreparedStatement insertStatement;

  SessionState(
      DseSession session, KafkaCodecRegistry codecRegistry, PreparedStatement insertStatement) {
    this.session = session;
    this.codecRegistry = codecRegistry;
    this.insertStatement = insertStatement;
  }

  DseSession getSession() {
    return session;
  }

  KafkaCodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  PreparedStatement getInsertStatement() {
    return insertStatement;
  }
}
