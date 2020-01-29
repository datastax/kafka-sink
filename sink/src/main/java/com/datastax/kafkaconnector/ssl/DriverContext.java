/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

/**
 * Specialization of DefaultDriverContext that allows the connector to use OpenSSL or SniSslEngine.
 */
public class DriverContext extends DefaultDriverContext {
  @Nullable private final SslConfig sslConfig;

  DriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      @Nullable SslConfig sslConfig) {
    super(configLoader, programmaticArguments);
    this.sslConfig = sslConfig;
  }

  @Override
  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    // If a JDK-based factory was provided through the public API, wrap it;
    // this can only happen in kafka-connector if a secure connect bundle was provided.
    if (getSslEngineFactory().isPresent()) {
      return getSslEngineFactory().map(JdkSslHandlerFactory::new);
    } else if (sslConfig != null) {
      return Optional.of(
          new OpenSslHandlerFactory(
              sslConfig.getSslContext(), sslConfig.requireHostnameValidation()));
    } else {
      throw new IllegalStateException(
          "Neither sslConfig nor secure bundle was provided to configure SslHandlerFactory");
    }
  }
}
