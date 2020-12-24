/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.sink.cloud;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.INFO;
import static ru.lanwen.wiremock.ext.WiremockResolver.Wiremock;

import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.cloud.SNIProxyServer;
import com.datastax.oss.dsbulk.tests.cloud.SNIProxyServerExtension;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.pulsar.sink.ccm.ITConnectorBase;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.lanwen.wiremock.ext.WiremockResolver;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(SNIProxyServerExtension.class)
@ExtendWith(WiremockResolver.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("medium")
public class CloudSniEndToEndIT extends ITConnectorBase {

  private final SNIProxyServer proxy;
  private final CqlSession session;
  private final LogInterceptor logs;

  public CloudSniEndToEndIT(
      SNIProxyServer proxy,
      CqlSession session,
      @LogCapture(level = INFO, value = TableConfig.class) LogInterceptor logs) {
    super(proxy.getContactPoints(), null, proxy.getLocalDatacenter(), session);
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
    performInsert(cl, parametersWithSecureBundle());

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
    performInsert(cl, parametersWithSecureBundle());

    // then
    assertThat(logs.getLoggedMessages())
        .doesNotContain("Cloud deployments reject consistency level");
  }

  @Test
  void should_insert_using_secure_bundle_from_http(@Wiremock WireMockServer server)
      throws IOException {
    // given
    server.stubFor(
        any(urlPathEqualTo("/secure-bundle.zip"))
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
    taskConfigs.add(
        makeCloudConnectorProperties("bigintcol=value.bigint", "types", extras, "mytopic", cl));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            null,
            new GenericRecordImpl().put("bigint", 5725368L),
            recordType);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  private Map<String, String> parametersWithSecureBundle(String secureBundlePath) {

    return ImmutableMap.<String, String>builder()
        .put(CassandraSinkConfig.SECURE_CONNECT_BUNDLE_OPT, secureBundlePath)
        .put(AuthenticatorConfig.USERNAME_OPT, "cassandra")
        .put(AuthenticatorConfig.PASSWORD_OPT, "cassandra")
        .build();
  }

  private Map<String, String> parametersWithSecureBundle() throws MalformedURLException {
    return parametersWithSecureBundle(proxy.getSecureBundlePath().toUri().toURL().toString());
  }
}
