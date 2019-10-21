/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This isn't used in the connector really; it's primarily useful for trying things out and
 * debugging logic in IntelliJ.
 */
public class Main {
  public static void main(String[] args) {
    DseSinkConnector conn = new DseSinkConnector();

    String mappingString = "f1=value.f1, f2=value.f2";
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("mapping", mappingString)
            .put("keyspace", "ks1")
            .put("table", "b")
            .build();
    try {
      conn.start(props);
      String value = "{\"f1\": 42, \"f2\": 96}";
      SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
      DseSinkTask task = new DseSinkTask();
      task.start(props);
      task.put(Collections.singletonList(record));
    } finally {
      conn.stop();
    }
  }
}
