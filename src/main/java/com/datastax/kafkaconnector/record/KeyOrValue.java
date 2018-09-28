/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.record;

import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;

/** The key or value part of a Kafka {@link SinkRecord}. */
public interface KeyOrValue {
  Set<String> fields();

  Object getFieldValue(String field);
}
