/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm.auth;

import static com.datastax.kafkaconnector.config.SslConfig.HOSTNAME_VALIDATION_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.KEYSTORE_PASSWORD_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.KEYSTORE_PATH_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.OPENSSL_KEY_CERT_CHAIN_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.OPENSSL_PRIVATE_KEY_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.PROVIDER_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.TRUSTSTORE_PASSWORD_OPT;
import static com.datastax.kafkaconnector.config.SslConfig.TRUSTSTORE_PATH_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.kafkaconnector.ccm.EndToEndCCMITBase;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
@CCMConfig(ssl = true)
@Tag("medium")
class SslEndToEndCCMIT extends EndToEndCCMITBase {
  public SslEndToEndCCMIT(CCMCluster ccm, @SessionConfig(ssl = true) CqlSession session) {
    super(ccm, session);
  }

  @Test
  void raw_bigint_value_without_hostname_validation() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "JDK")
            .put(KEYSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath())
            .put(KEYSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .put(HOSTNAME_VALIDATION_OPT, "false")
            .put(TRUSTSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_with_hostname_validation() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "JDK")
            .put(KEYSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath())
            .put(KEYSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .put(TRUSTSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(AllNodesFailedException.class);
  }

  @Test
  void raw_bigint_value_with_openssl_without_hostname_validation() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "OpenSSL")
            .put(HOSTNAME_VALIDATION_OPT, "false")
            .put(
                OPENSSL_KEY_CERT_CHAIN_OPT,
                CcmBridge.DEFAULT_CLIENT_CERT_CHAIN_FILE.getAbsolutePath())
            .put(
                OPENSSL_PRIVATE_KEY_OPT,
                CcmBridge.DEFAULT_CLIENT_PRIVATE_KEY_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_openssl_with_hostname_validation() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "OpenSSL")
            .put(
                OPENSSL_KEY_CERT_CHAIN_OPT,
                CcmBridge.DEFAULT_CLIENT_CERT_CHAIN_FILE.getAbsolutePath())
            .put(
                OPENSSL_PRIVATE_KEY_OPT,
                CcmBridge.DEFAULT_CLIENT_PRIVATE_KEY_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(AllNodesFailedException.class);
  }
}
