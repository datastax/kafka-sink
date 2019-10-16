/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.cloud;

import static com.datastax.kafkaconnector.config.AuthenticatorConfig.PASSWORD_OPT;
import static com.datastax.kafkaconnector.config.AuthenticatorConfig.USERNAME_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.SECURE_CONNECT_BUNDLE_OPT;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.INFO;
import static ru.lanwen.wiremock.ext.WiremockResolver.*;

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
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
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
import ru.lanwen.wiremock.ext.WiremockResolver;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith({SNIProxyServerExtension.class})
@ExtendWith(WiremockResolver.class)
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
    super(proxy.getContactPoints(), null, proxy.getLocalDCName(), session);
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
    performInsert(cl, parametersWithSecureBundle());

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
    performInsert(cl, parametersWithSecureBundle());

    // then
    assertThat(logs.getLoggedMessages())
        .doesNotContain("Cloud deployments reject consistency level");
  }

  @Test
  void should_connect_and_insert_to_cloud_overriding_wrong_cl_secure_bundle_without_password()
      throws MalformedURLException {
    // given
    DefaultConsistencyLevel cl = DefaultConsistencyLevel.ONE;

    // when
    performInsert(cl, parametersWithSecureBundleUsernameAndPassword());

    // then
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                cl));
  }

  @Test
  void should_connect_and_insert_to_cloud_using_correct_cl_secure_bundle_without_password()
      throws MalformedURLException {
    // given
    DefaultConsistencyLevel cl = DefaultConsistencyLevel.LOCAL_QUORUM;

    // when
    performInsert(cl, parametersWithSecureBundleUsernameAndPassword());

    // then
    assertThat(logs.getLoggedMessages())
        .doesNotContain("Cloud deployments reject consistency level");
  }

  @Test
  void should_insert_using_secure_bundle_from_http(@Wiremock WireMockServer server)
      throws IOException {
    // given
    server.stubFor(
        any(urlEqualTo("secure-bundle.zip"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(Files.readAllBytes(proxy.getSecureBundlePath()))));

    URL configFile = new URL(String.format("%s/%s", server.baseUrl(), "secure-bundle.zip"));

    // when
    performInsert(
        DefaultConsistencyLevel.LOCAL_QUORUM,
        parametersWithSecureBundle(configFile.toExternalForm()));

    // then
    assertThat(logs.getLoggedMessages())
        .doesNotContain("Cloud deployments reject consistency level");
  }

  private void performInsert(ConsistencyLevel cl, Map<String, String> extras) {
    conn.start(makeCloudConnectorProperties("bigintcol=value", "types", extras, "mytopic", cl));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  private Map<String, String> parametersWithSecureBundle(String secureBundlePath) {
    return ImmutableMap.<String, String>builder()
        .put(SECURE_CONNECT_BUNDLE_OPT, secureBundlePath)
        .build();
  }

  private Map<String, String> parametersWithSecureBundle() throws MalformedURLException {
    return parametersWithSecureBundle(proxy.getSecureBundlePath().toUri().toURL().toString());
  }

  private Map<String, String> parametersWithSecureBundleUsernameAndPassword()
      throws MalformedURLException {
    return ImmutableMap.<String, String>builder()
        .put(
            SECURE_CONNECT_BUNDLE_OPT,
            proxy.getSecureBundleWithoutUsernamePassword().toUri().toURL().toString())
        .put(USERNAME_OPT, proxy.getUsername())
        .put(PASSWORD_OPT, proxy.getPassword())
        .build();
  }
}
