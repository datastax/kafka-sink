/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.api.core.session.DseProgrammaticArguments;
import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import java.util.Optional;

/** Session builder specialization that hooks in OpenSSL when that ssl provider is chosen. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SessionBuilder extends DseSessionBuilder {
  private final Optional<SslConfig> sslConfig;

  public SessionBuilder(Optional<SslConfig> sslConfig) {
    this.sslConfig = sslConfig;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    // DseSessionBuilder.buildContext has some side-effects (adding dse type-codecs to typeCodecs)
    // that we also need.
    DriverContext baseContext = super.buildContext(configLoader, programmaticArguments);

    // If sslConfig is not provided we need to setup custom driver context for cloud
    if (sslConfig.map(s -> s.getProvider() != SslConfig.Provider.OpenSSL).orElse(false)) {
      // We're not using OpenSSL so the normal driver context is fine to use.
      return baseContext;
    }

    return new com.datastax.kafkaconnector.ssl.DriverContext(
        configLoader, programmaticArguments, DseProgrammaticArguments.builder().build(), sslConfig);
  }
}
