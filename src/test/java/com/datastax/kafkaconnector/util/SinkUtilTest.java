/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SinkUtilTest {
  @Test
  void should_serialize_and_deserialize() {
    Map<String, Map<CqlIdentifier, CqlIdentifier>> mappings =
        ImmutableMap.<String, Map<CqlIdentifier, CqlIdentifier>>builder()
            .put(
                "topic1",
                ImmutableMap.<CqlIdentifier, CqlIdentifier>builder()
                    .put(CqlIdentifier.fromInternal("c1"), CqlIdentifier.fromInternal("value.f1"))
                    .put(CqlIdentifier.fromInternal("c2"), CqlIdentifier.fromInternal("value.f2"))
                    .build())
            .put(
                "topic2",
                ImmutableMap.<CqlIdentifier, CqlIdentifier>builder()
                    .put(CqlIdentifier.fromInternal("d1"), CqlIdentifier.fromInternal("value.g1"))
                    .put(CqlIdentifier.fromInternal("d2"), CqlIdentifier.fromInternal("value.g2"))
                    .build())
            .build();

    DseSinkConfig config =
        new DseSinkConfig(
            ImmutableMap.<String, String>builder()
                .put("topic.topic1.keyspace", "ks")
                .put("topic.topic1.table", "table")
                .put("topic.topic1.mapping", "c1=value.f1, c2=value.f2")
                .put("topic.topic2.keyspace", "ks")
                .put("topic.topic2.table", "table")
                .put("topic.topic2.mapping", "d1=value.g1, d2=value.g2")
                .build());
    Map<String, Map<CqlIdentifier, CqlIdentifier>> result =
        SinkUtil.deserializeTopicMappings(SinkUtil.serializeTopicMappings(config));
    assertThat(result).isEqualTo(mappings);
  }
}
