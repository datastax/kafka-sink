/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import java.util.StringTokenizer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class MetricsJmxReporter {
  private static final String DATASTAX_KAFKA_CONNECTOR_PREFIX = "datastax.kafkaconnector.";

  public static JmxReporter createJmxReporter(String instanceName, MetricRegistry metricRegistry) {
    return JmxReporter.forRegistry(metricRegistry)
        .inDomain(DATASTAX_KAFKA_CONNECTOR_PREFIX + instanceName)
        .createsObjectNamesWith(
            (type, domain, name) -> {
              try {
                StringBuilder sb =
                    new StringBuilder("com.datastax.kafkaconnector:0=")
                        .append(instanceName)
                        .append(',');
                StringTokenizer tokenizer = new StringTokenizer(name, "/");
                int i = 1;
                while (tokenizer.hasMoreTokens()) {
                  String token = tokenizer.nextToken();
                  if (tokenizer.hasMoreTokens()) {
                    sb.append(i++).append('=').append(token).append(',');
                  } else {
                    sb.append("name=").append(token);
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
