/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.kafkaconnector.config.DseSinkConfig.SECURE_CONNECT_BUNDLE_OPT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.kafkaconnector.cloud.SNIProxyServer;
import com.datastax.kafkaconnector.cloud.SNIProxyServerExtension;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.collect.ImmutableMap;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({SNIProxyServerExtension.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CloudSniEndToEndCCMIT extends CCMITConnectorBase {

  private final SNIProxyServer proxy;
  private CqlSession session;

  public CloudSniEndToEndCCMIT(SNIProxyServer proxy, CqlSession session) {
    super(proxy.getContactPoints(), proxy.getPort(), proxy.getLocalDCName(), session);
    this.proxy = proxy;
    this.session = session;
  }

  @BeforeAll
  void createTables() {
    session.execute(
        SimpleStatement.builder("CREATE TABLE IF NOT EXISTS types (bigintCol bigint PRIMARY KEY)")
            .setTimeout(Duration.ofSeconds(30))
            .build());
  }

  @BeforeEach
  void truncateTable() {
    session.execute("TRUNCATE types");
  }

  @Test
  void should_connect_to_cloud_using() throws MalformedURLException {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, proxy.getSecureBundlePath().toUri().toURL().toString())
            .build();

    conn.start(makeCloudConnectorProperties("bigintcol=value", "types", extras, "mytopic"));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }
}
