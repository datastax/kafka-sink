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
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import java.util.Optional;

/** Specialization of DseDriverContext that allows the connector to use OpenSSL. */
public class DriverContext extends DseDriverContext {
  private final SslConfig sslConfig;

  DriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      DseProgrammaticArguments dseProgrammaticArguments,
      SslConfig sslConfig) {
    super(configLoader, programmaticArguments, dseProgrammaticArguments);
    this.sslConfig = sslConfig;
  }

  @Override
  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    return Optional.of(
        new OpenSslHandlerFactory(
            sslConfig.getSslContext(), sslConfig.requireHostnameValidation()));
  }
}
