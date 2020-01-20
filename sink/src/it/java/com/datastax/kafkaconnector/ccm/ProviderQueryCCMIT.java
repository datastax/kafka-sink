/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
public class ProviderQueryCCMIT extends EndToEndCCMITBase {
  private AttachmentPoint attachmentPoint;

  public ProviderQueryCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
    attachmentPoint =
        new AttachmentPoint() {
          @NotNull
          @Override
          public ProtocolVersion getProtocolVersion() {
            return session.getContext().getProtocolVersion();
          }

          @NotNull
          @Override
          public CodecRegistry getCodecRegistry() {
            return session.getContext().getCodecRegistry();
          }
        };
  }

  @Test
  void should_insert_json_using_query_parameter() {
    String query =
        String.format(
            "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)", keyspaceName);

    conn.start(makeConnectorProperties("bigintcol=value.bigint, " + "intcol=value.int", query));

    String value = "{\"bigint\": 1234, \"int\": 10000}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
  }
}
