/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.dse.driver.api.core.session.DseProgrammaticArguments;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import java.util.Optional;

/** Specialization of DseDriverContext that allows the connector to use OpenSSL or SniSslEngine. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class DriverContext extends DseDriverContext {
  private final Optional<SslConfig> sslConfig;

  DriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      DseProgrammaticArguments dseProgrammaticArguments,
      Optional<SslConfig> sslConfig) {
    super(configLoader, programmaticArguments, dseProgrammaticArguments);
    this.sslConfig = sslConfig;
  }

  @Override
  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    // If a JDK-based factory was provided through the public API, wrap it;
    // this can only happen in kafka-connector if a secure connect bundle was provided.
    if (getSslEngineFactory().isPresent()) {
      return getSslEngineFactory().map(JdkSslHandlerFactory::new);
    } else if (sslConfig.isPresent()) {
      return Optional.of(
          new OpenSslHandlerFactory(
              sslConfig.get().getSslContext(), sslConfig.get().requireHostnameValidation()));
    } else {
      throw new IllegalArgumentException(
          "The sslConfig and secure bundle was not provided to configure SslHandlerFactory");
    }
  }
}
