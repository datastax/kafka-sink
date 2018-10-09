/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** Session builder specialization that hooks in OpenSSL when that ssl provider is chosen. */
public class SessionBuilder extends DseSessionBuilder {
  private final SslConfig sslConfig;

  public SessionBuilder(SslConfig sslConfig) {
    this.sslConfig = sslConfig;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    // DseSessionBuilder.buildContext has some side-effects (adding dse type-codecs to typeCodecs)
    // that we also need.
    DriverContext baseContext =
        super.buildContext(
            configLoader,
            typeCodecs,
            nodeStateListener,
            schemaChangeListener,
            requestTracker,
            nodeFilters,
            classLoader);

    if (sslConfig.getProvider() != SslConfig.Provider.OpenSSL) {
      // We're not using OpenSSL so the normal driver context is fine to use.
      return baseContext;
    }

    return new com.datastax.kafkaconnector.ssl.DriverContext(
        configLoader,
        typeCodecs,
        nodeStateListener,
        schemaChangeListener,
        requestTracker,
        nodeFilters,
        classLoader,
        null,
        null,
        null,
        sslConfig);
  }
}
