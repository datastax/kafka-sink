/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import static com.datastax.dsbulk.commons.internal.utils.StringUtils.quoteJMXIfNecessary;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.base.Splitter;
import java.util.Iterator;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class MetricsJmxReporter {
  private static final String CONNECTOR_DOMAIN = "com.datastax.kafkaconnector";

  public static JmxReporter createJmxReporter(String instanceName, MetricRegistry metricRegistry) {
    return JmxReporter.forRegistry(metricRegistry)
        .inDomain(CONNECTOR_DOMAIN)
        .createsObjectNamesWith(
            (metricType, jmxDomain, metricName) -> {
              try {
                StringBuilder sb =
                    new StringBuilder(jmxDomain)
                        .append(":connector=")
                        .append(quoteJMXIfNecessary(instanceName))
                        .append(',');
                Iterator<String> tokens = Splitter.on("/").split(metricName).iterator();
                if (metricName.contains("batchSize")) {
                  // special-case batchSize metrics and expose them per topic, ks and table
                  sb.append("topic=")
                      .append(quoteJMXIfNecessary(tokens.next()))
                      .append(",keyspace=")
                      .append(quoteJMXIfNecessary(tokens.next()))
                      .append(",table=")
                      .append(quoteJMXIfNecessary(tokens.next()))
                      .append(",name=")
                      .append(quoteJMXIfNecessary(tokens.next()));
                } else if (metricName.contains("driver")) {
                  // special-case driver metrics and expose them per session
                  sb.append("driver=").append(tokens.next());
                  Iterator<String> sessionAndMetric =
                      Splitter.on('.').split(tokens.next()).iterator();
                  sb.append(",session=")
                      .append(quoteJMXIfNecessary(sessionAndMetric.next()))
                      .append(",name=")
                      .append(quoteJMXIfNecessary(sessionAndMetric.next()));
                } else {
                  // other metrics get a generic path
                  int i = 1;
                  while (tokens.hasNext()) {
                    String token = tokens.next();
                    if (tokens.hasNext()) {
                      sb.append("level").append(i++);
                    } else {
                      sb.append("name");
                    }
                    sb.append('=').append(quoteJMXIfNecessary(token));
                    if (tokens.hasNext()) {
                      sb.append(',');
                    }
                  }
                }
                return new ObjectName(sb.toString());
              } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
              }
            })
        .build();
  }
}
