/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.ssl;

import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

public class OpenSslHandlerFactory implements SslHandlerFactory {
  private final SslContext context;
  private final boolean requireHostValidation;

  OpenSslHandlerFactory(SslContext context, boolean requireHostValidation) {
    this.context = context;
    this.requireHostValidation = requireHostValidation;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, SocketAddress socketAddress) {
    SslHandler sslHandler;
    if (socketAddress instanceof InetSocketAddress) {
      InetSocketAddress inetAddr = (InetSocketAddress) socketAddress;
      sslHandler = context.newHandler(channel.alloc(), inetAddr.getHostName(), inetAddr.getPort());
    } else {
      sslHandler = context.newHandler(channel.alloc());
    }

    if (requireHostValidation) {
      SSLEngine sslEngine = sslHandler.engine();
      SSLParameters sslParameters = sslEngine.getSSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
      sslEngine.setSSLParameters(sslParameters);
    }
    return sslHandler;
  }

  @Override
  public void close() {
    // No-op
  }
}
