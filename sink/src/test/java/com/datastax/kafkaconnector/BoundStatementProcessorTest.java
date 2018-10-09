/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.kafkaconnector.record.RecordAndStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class BoundStatementProcessorTest {
  @Test
  void should_categorize_statement_in_statement_group() {
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, "value", 1234L);

    BoundStatement bs1 = mock(BoundStatement.class);
    ByteBuffer routingKey = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(bs1.getRoutingKey()).thenReturn(routingKey);

    RecordAndStatement recordAndStatement = new RecordAndStatement(record, "ks.mytable", bs1);
    Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups = new HashMap<>();

    // We don't care about the args to the constructor for this test.
    BoundStatementProcessor statementProcessor = new BoundStatementProcessor(null, null, null);
    List<RecordAndStatement> result =
        statementProcessor.categorizeStatement(statementGroups, recordAndStatement);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isSameAs(recordAndStatement);
    assertThat(statementGroups.size()).isEqualTo(1);
    assertThat(statementGroups.containsKey("ks.mytable")).isTrue();
    Map<ByteBuffer, List<RecordAndStatement>> batchGroups = statementGroups.get("ks.mytable");
    assertThat(batchGroups.size()).isEqualTo(1);
    assertThat(batchGroups.containsKey(routingKey)).isTrue();
    List<RecordAndStatement> batchGroup = batchGroups.get(routingKey);
    assertThat(batchGroup).isSameAs(result);
  }
}
