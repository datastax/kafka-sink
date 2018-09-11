/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Topic-specific connector configuration. */
public class SslConfig extends AbstractConfig {
  public static final String PROVIDER_OPT = "ssl.provider";
  public static final String HOSTNAME_VALIDATION_OPT = "ssl.hostnameValidation";
  public static final String KEYSTORE_PASSWORD_OPT = "ssl.keystore.password";
  public static final String KEYSTORE_PATH_OPT = "ssl.keystore.path";
  public static final String OPENSSL_KEY_CERT_CHAIN_OPT = "ssl.openssl.keyCertChain";
  public static final String OPENSSL_PRIVATE_KEY_OPT = "ssl.openssl.privateKey";
  public static final String TRUSTSTORE_PASSWORD_OPT = "ssl.truststore.password";
  public static final String TRUSTSTORE_PATH_OPT = "ssl.truststore.path";
  static final String CIPHER_SUITES_OPT = "ssl.cipherSuites";

  private static final Path EMPTY_PATH = Paths.get("");
  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              PROVIDER_OPT,
              ConfigDef.Type.STRING,
              "None",
              ConfigDef.Importance.HIGH,
              "None | JDK | OpenSSL")
          .define(
              CIPHER_SUITES_OPT,
              ConfigDef.Type.LIST,
              Collections.EMPTY_LIST,
              ConfigDef.Importance.HIGH,
              "The cipher suites to enable")
          .define(
              HOSTNAME_VALIDATION_OPT,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.HIGH,
              "Whether or not to validate DSE node hostnames when using SSL")
          .define(
              KEYSTORE_PASSWORD_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Keystore password")
          .define(
              KEYSTORE_PATH_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the keystore file")
          .define(
              OPENSSL_KEY_CERT_CHAIN_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the certificate chain file")
          .define(
              OPENSSL_PRIVATE_KEY_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the private key file")
          .define(
              TRUSTSTORE_PASSWORD_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Truststore password")
          .define(
              TRUSTSTORE_PATH_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the truststore file");

  private final Path keystorePath;
  private final Path truststorePath;
  private final Path certFilePath;
  private final Path privateKeyPath;
  private final SslContext sslContext;

  SslConfig(Map<String, String> sslSettings) {
    super(CONFIG_DEF, sslSettings, false);

    keystorePath = getFilePath(KEYSTORE_PATH_OPT);
    truststorePath = getFilePath(TRUSTSTORE_PATH_OPT);
    privateKeyPath = getFilePath(OPENSSL_PRIVATE_KEY_OPT);
    certFilePath = getFilePath(OPENSSL_KEY_CERT_CHAIN_OPT);

    assertAccessibleFile(keystorePath, KEYSTORE_PATH_OPT);
    assertAccessibleFile(truststorePath, TRUSTSTORE_PATH_OPT);
    assertAccessibleFile(privateKeyPath, OPENSSL_PRIVATE_KEY_OPT);
    assertAccessibleFile(certFilePath, OPENSSL_KEY_CERT_CHAIN_OPT);

    if (getProvider() == Provider.OpenSSL) {
      TrustManagerFactory tmf = null;
      String sslTrustStorePassword = getTruststorePassword();
      try {
        if (truststorePath != EMPTY_PATH) {
          KeyStore ks = KeyStore.getInstance("JKS");
          try {
            ks.load(
                new BufferedInputStream(new FileInputStream(truststorePath.toFile())),
                sslTrustStorePassword.toCharArray());
          } catch (IOException e) {
            if (e.getMessage() == null) {
              throw new KafkaException("Invalid truststore", e);
            } else {
              throw new KafkaException(String.format("Invalid truststore: %s", e.getMessage()), e);
            }
          }

          tmf = TrustManagerFactory.getInstance("SunX509");
          tmf.init(ks);
        }

        List<String> cipherSuites = getCipherSuites();

        SslContextBuilder builder =
            SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);
        if (!cipherSuites.isEmpty()) {
          builder.ciphers(cipherSuites);
        }
        if (getOpenSslKeyCertChain() != null) {
          try {
            builder.keyManager(
                new BufferedInputStream(new FileInputStream(getOpenSslKeyCertChain().toFile())),
                new BufferedInputStream(new FileInputStream(getOpenSslPrivateKey().toFile())));
          } catch (IllegalArgumentException e) {
            throw new KafkaException(
                String.format("Invalid certificate or private key: %s", e.getMessage()), e);
          }
        }
        this.sslContext = builder.build();
      } catch (GeneralSecurityException
          | FileNotFoundException
          | RuntimeException
          | SSLException e) {
        throw new KafkaException(
            String.format("Could not initialize OpenSSL context: %s", e.getMessage()), e);
      }
    } else {
      sslContext = null;
    }
  }

  public Provider getProvider() {
    String providerString = getString(PROVIDER_OPT);
    try {
      return Provider.valueOf(providerString);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          PROVIDER_OPT, providerString, "valid values are None, JDK, OpenSSL");
    }
  }

  public List<String> getCipherSuites() {
    return getList(CIPHER_SUITES_OPT);
  }

  public boolean requireHostnameValidation() {
    return getBoolean(HOSTNAME_VALIDATION_OPT);
  }

  public Path getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return getString(KEYSTORE_PASSWORD_OPT);
  }

  public Path getTruststorePath() {
    return truststorePath;
  }

  public String getTruststorePassword() {
    return getString(TRUSTSTORE_PASSWORD_OPT);
  }

  public SslContext getSslContext() {
    return sslContext;
  }

  @Override
  public String toString() {
    Set<String> secureSslSettings =
        new HashSet<>(Arrays.asList(KEYSTORE_PASSWORD_OPT, TRUSTSTORE_PASSWORD_OPT));
    String[] sslSettings = {
      PROVIDER_OPT,
      KEYSTORE_PATH_OPT,
      KEYSTORE_PASSWORD_OPT,
      TRUSTSTORE_PATH_OPT,
      TRUSTSTORE_PASSWORD_OPT,
      OPENSSL_KEY_CERT_CHAIN_OPT,
      OPENSSL_PRIVATE_KEY_OPT
    };
    String sslString =
        Arrays.stream(sslSettings)
            .map(
                s ->
                    String.format(
                        "%s: %s",
                        s.substring("ssl.".length()),
                        secureSslSettings.contains(s) ? "<hidden>" : getString(s)))
            .collect(Collectors.joining("\n"));

    return String.format(
        "%s%n%s: %s",
        sslString,
        HOSTNAME_VALIDATION_OPT.substring("ssl.".length()),
        getBoolean(HOSTNAME_VALIDATION_OPT));
  }

  Path getOpenSslKeyCertChain() {
    return certFilePath;
  }

  Path getOpenSslPrivateKey() {
    return privateKeyPath;
  }

  private static void assertAccessibleFile(Path filePath, String settingName) {
    if (filePath.equals(EMPTY_PATH)) {
      // There's no path to check.
      return;
    }

    if (!Files.exists(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "does not exist");
    }
    if (!Files.isRegularFile(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not a file");
    }
    if (!Files.isReadable(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not readable");
    }
  }

  private Path getFilePath(String setting) {
    String settingValue = getString(setting);
    return settingValue.isEmpty()
        ? Paths.get("")
        : Paths.get(settingValue).toAbsolutePath().normalize();
  }

  public enum Provider {
    None,
    JDK,
    OpenSSL
  }
}
