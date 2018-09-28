/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.record;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A parsed {@link SinkRecord} that is ready to have field mapping applied to it for inserting into
 * DSE.
 */
public interface Record extends KeyOrValue {
  Long getTimestamp();
}
