/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

/** Specialization of DseDriverContext that allows the connector to use OpenSSL. */
public class DriverContext extends DseDriverContext {
  private final SslConfig sslConfig;

  DriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader,
      UUID clientId,
      String applicationName,
      String applicationVersion,
      SslConfig sslConfig) {
    super(
        configLoader,
        typeCodecs,
        nodeStateListener,
        schemaChangeListener,
        requestTracker,
        nodeFilters,
        classLoader,
        clientId,
        applicationName,
        applicationVersion);
    this.sslConfig = sslConfig;
  }

  @Override
  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    return Optional.of(
        new OpenSslHandlerFactory(
            sslConfig.getSslContext(), sslConfig.requireHostnameValidation()));
  }
}
