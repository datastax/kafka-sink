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
package com.datastax.oss.kafka.sink.config;

import static com.datastax.oss.driver.api.testinfra.ccm.CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.oss.kafka.sink.config.SslConfig.CIPHER_SUITES_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.HOSTNAME_VALIDATION_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.KEYSTORE_PASSWORD_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.KEYSTORE_PATH_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.OPENSSL_KEY_CERT_CHAIN_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.OPENSSL_PRIVATE_KEY_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.PROVIDER_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.TRUSTSTORE_PASSWORD_OPT;
import static com.datastax.oss.kafka.sink.config.SslConfig.TRUSTSTORE_PATH_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SslConfigTest {
  private static Path keystorePath;
  private static Path truststorePath;
  private static Path certfilePath;
  private static Path privateKeyPath;

  @BeforeAll
  static void createSecurityFiles() throws IOException {
    keystorePath = Files.createTempFile("keystore", "");
    truststorePath = Files.createTempFile("truststore", "");
    certfilePath = Files.createTempFile("client", "cert");
    privateKeyPath = Files.createTempFile("client", "key");
  }

  @Test
  void should_parse_settings() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "JDK")
            .put(CIPHER_SUITES_OPT, "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
            .put(HOSTNAME_VALIDATION_OPT, "true")
            .put(KEYSTORE_PASSWORD_OPT, "pass1")
            .put(KEYSTORE_PATH_OPT, keystorePath.toString())
            .put(OPENSSL_KEY_CERT_CHAIN_OPT, certfilePath.toString())
            .put(OPENSSL_PRIVATE_KEY_OPT, privateKeyPath.toString())
            .put(TRUSTSTORE_PASSWORD_OPT, "pass2")
            .put(TRUSTSTORE_PATH_OPT, truststorePath.toString())
            .build();
    SslConfig sslConfig = new SslConfig(props);
    assertThat(sslConfig.getProvider()).isEqualTo(SslConfig.Provider.JDK);
    assertThat(sslConfig.getCipherSuites())
        .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
    assertThat(sslConfig.requireHostnameValidation()).isTrue();
    assertThat(sslConfig.getKeystorePassword()).isEqualTo("pass1");
    assertThat(sslConfig.getKeystorePath()).isEqualTo(keystorePath.toAbsolutePath().normalize());
    assertThat(sslConfig.getOpenSslKeyCertChain())
        .isEqualTo(certfilePath.toAbsolutePath().normalize());
    assertThat(sslConfig.getOpenSslPrivateKey())
        .isEqualTo(privateKeyPath.toAbsolutePath().normalize());
    assertThat(sslConfig.getTruststorePassword()).isEqualTo("pass2");
    assertThat(sslConfig.getTruststorePath())
        .isEqualTo(truststorePath.toAbsolutePath().normalize());
  }

  @Test
  void should_parse_settings_empty_file_settings() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "JDK")
            .put(CIPHER_SUITES_OPT, "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
            .put(HOSTNAME_VALIDATION_OPT, "true")
            .put(KEYSTORE_PASSWORD_OPT, "pass1")
            .put(KEYSTORE_PATH_OPT, "")
            .put(OPENSSL_KEY_CERT_CHAIN_OPT, "")
            .put(OPENSSL_PRIVATE_KEY_OPT, "")
            .put(TRUSTSTORE_PASSWORD_OPT, "pass2")
            .put(TRUSTSTORE_PATH_OPT, "")
            .build();
    SslConfig sslConfig = new SslConfig(props);
    assertThat(sslConfig.getProvider()).isEqualTo(SslConfig.Provider.JDK);
    assertThat(sslConfig.getCipherSuites())
        .containsExactly("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
    assertThat(sslConfig.requireHostnameValidation()).isTrue();
    assertThat(sslConfig.getKeystorePassword()).isEqualTo("pass1");
    assertThat(sslConfig.getKeystorePath()).isNull();
    assertThat(sslConfig.getOpenSslKeyCertChain()).isNull();
    assertThat(sslConfig.getOpenSslPrivateKey()).isNull();
    assertThat(sslConfig.getTruststorePassword()).isEqualTo("pass2");
    assertThat(sslConfig.getTruststorePath()).isNull();
  }

  @Test
  void should_error_non_existent_files() {
    String badPath = Paths.get("foo").toAbsolutePath().normalize().toString();
    Arrays.stream(
            new String[] {
              KEYSTORE_PATH_OPT,
              TRUSTSTORE_PATH_OPT,
              OPENSSL_PRIVATE_KEY_OPT,
              OPENSSL_KEY_CERT_CHAIN_OPT
            })
        .forEach(
            s -> {
              Map<String, String> props =
                  ImmutableMap.<String, String>builder().put(s, "foo").build();
              assertThatThrownBy(() -> new SslConfig(props))
                  .isInstanceOf(ConfigException.class)
                  .hasMessage(
                      String.format(
                          "Invalid value %s for configuration %s: does not exist", badPath, s));
            });
  }

  @Test
  void should_error_private_key_without_cert() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(PROVIDER_OPT, "OpenSSL")
              //              .put(OPENSSL_KEY_CERT_CHAIN_OPT, certfilePath.toString())
              .put(OPENSSL_PRIVATE_KEY_OPT, privateKeyPath.toString())
              .build();
      assertThatThrownBy(() -> new SslConfig(props))
          .isInstanceOf(KafkaException.class)
          .hasMessageContaining(
              String.format(
                  "%s cannot be set without %s and vice-versa: %s is not set",
                  OPENSSL_KEY_CERT_CHAIN_OPT, OPENSSL_PRIVATE_KEY_OPT, OPENSSL_KEY_CERT_CHAIN_OPT));
    }
  }

  @Test
  void should_error_cert_without_private_key() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(PROVIDER_OPT, "OpenSSL")
              .put(OPENSSL_KEY_CERT_CHAIN_OPT, certfilePath.toString())
              .build();
      assertThatThrownBy(() -> new SslConfig(props))
          .isInstanceOf(KafkaException.class)
          .hasMessageContaining(
              String.format(
                  "%s cannot be set without %s and vice-versa: %s is not set",
                  OPENSSL_KEY_CERT_CHAIN_OPT, OPENSSL_PRIVATE_KEY_OPT, OPENSSL_PRIVATE_KEY_OPT));
    }
  }

  @Test
  void should_error_bad_ssl_files() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(PROVIDER_OPT, "OpenSSL")
              .put(CIPHER_SUITES_OPT, "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
              .put(HOSTNAME_VALIDATION_OPT, "true")
              .put(KEYSTORE_PASSWORD_OPT, "pass1")
              .put(KEYSTORE_PATH_OPT, keystorePath.toString())
              .put(OPENSSL_KEY_CERT_CHAIN_OPT, certfilePath.toString())
              .put(OPENSSL_PRIVATE_KEY_OPT, privateKeyPath.toString())
              .put(TRUSTSTORE_PASSWORD_OPT, "pass2")
              .put(TRUSTSTORE_PATH_OPT, truststorePath.toString())
              .build();
      assertThatThrownBy(() -> new SslConfig(props))
          .isInstanceOf(KafkaException.class)
          .hasMessageContaining("Invalid truststore");
    }
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(PROVIDER_OPT, "OpenSSL")
              .put(CIPHER_SUITES_OPT, "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
              .put(HOSTNAME_VALIDATION_OPT, "true")
              .put(KEYSTORE_PASSWORD_OPT, "pass1")
              .put(KEYSTORE_PATH_OPT, keystorePath.toString())
              .put(OPENSSL_KEY_CERT_CHAIN_OPT, certfilePath.toString())
              .put(OPENSSL_PRIVATE_KEY_OPT, privateKeyPath.toString())
              .put(TRUSTSTORE_PASSWORD_OPT, DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
              .put(TRUSTSTORE_PATH_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
              .build();
      assertThatThrownBy(() -> new SslConfig(props))
          .isInstanceOf(KafkaException.class)
          .hasMessageContaining("Invalid certificate or private key");
    }
  }

  @Test
  void should_error_invalid_provider() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(PROVIDER_OPT, "foo").build();
    assertThatThrownBy(() -> new SslConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Invalid value foo for configuration ssl.provider: "
                + "valid values are None, JDK, OpenSSL");
  }
}
