/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.metadata;

import com.datastax.kafkaconnector.record.KeyOrValue;
import com.datastax.kafkaconnector.record.RecordMetadata;
import org.apache.kafka.connect.sink.SinkRecord;

/** Simple container class to tie together a {@link SinkRecord} key/value and its metadata. */
public class InnerDataAndMetadata {
  private final KeyOrValue innerData;
  private final RecordMetadata innerMetadata;

  InnerDataAndMetadata(KeyOrValue innerData, RecordMetadata innerMetadata) {
    this.innerMetadata = innerMetadata;
    this.innerData = innerData;
  }

  public KeyOrValue getInnerData() {
    return innerData;
  }

  public RecordMetadata getInnerMetadata() {
    return innerMetadata;
  }
}
