/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.cloud;

import static com.datastax.kafkaconnector.config.DseSinkConfig.SECURE_CONNECT_BUNDLE_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.INFO;

import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.kafkaconnector.ccm.ITConnectorBase;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith({SNIProxyServerExtension.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("medium")
public class CloudSniEndToEndIT extends ITConnectorBase {

  private final SNIProxyServer proxy;
  private CqlSession session;
  private LogInterceptor logs;

  public CloudSniEndToEndIT(
      SNIProxyServer proxy,
      CqlSession session,
      @LogCapture(level = INFO, value = TableConfig.class) LogInterceptor logs) {
    super(proxy.getContactPoints(), proxy.getPort(), proxy.getLocalDCName(), session);
    this.proxy = proxy;
    this.session = session;
    this.logs = logs;
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

  @BeforeEach
  void clearLogs() {
    logs.clear();
  }

  @Test
  void should_connect_and_insert_to_cloud_overriding_wrong_cl() throws MalformedURLException {
    // given
    DefaultConsistencyLevel cl = DefaultConsistencyLevel.ONE;

    // when
    performInsert(cl);

    // then
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                cl));
  }

  @Test
  void should_connect_and_insert_to_cloud_using_correct_cl() throws MalformedURLException {
    // given
    DefaultConsistencyLevel cl = DefaultConsistencyLevel.LOCAL_QUORUM;

    // when
    performInsert(cl);

    // then
    assertThat(logs.getLoggedMessages())
        .doesNotContain("Cloud deployments reject consistency level");
  }

  private void performInsert(ConsistencyLevel cl) throws MalformedURLException {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, proxy.getSecureBundlePath().toUri().toURL().toString())
            .build();

    conn.start(makeCloudConnectorProperties("bigintcol=value", "types", extras, "mytopic", cl));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }
}
