/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.sink.metrics;

import static com.datastax.oss.dsbulk.commons.utils.StringUtils.quoteJMXIfNecessary;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsJmxReporter {
  private static final String CONNECTOR_DOMAIN = "com.datastax.kafkaconnector";
  private static final Logger log = LoggerFactory.getLogger(MetricsJmxReporter.class);

  public static JmxReporter createJmxReporter(String instanceName, MetricRegistry metricRegistry) {
    return JmxReporter.forRegistry(metricRegistry)
        .inDomain(CONNECTOR_DOMAIN)
        .createsObjectNamesWith(
            (ignore, jmxDomain, metricName) -> getObjectName(instanceName, jmxDomain, metricName))
        .build();
  }

  @VisibleForTesting
  @NonNull
  static ObjectName getObjectName(String instanceName, String jmxDomain, String metricName) {
    log.debug(
        "registering JMX objectName - instanceName: {}, jmxDomain: {}, metricName: {}",
        instanceName,
        jmxDomain,
        metricName);
    try {
      StringBuilder sb =
          new StringBuilder(jmxDomain)
              .append(":connector=")
              .append(quoteJMXIfNecessary(instanceName))
              .append(',');
      Iterator<String> tokens = Splitter.on("/").split(metricName).iterator();
      if (metricName.contains("batchSize")
          || metricName.contains("batchSizeInBytes")
          || metricName.contains("failedRecordCount")
          || metricName.contains("recordCount")) {
        // special-case batchSize, batchSizeInBytes, failedRecordCount, recordCount metrics
        // and expose them per topic, ks and table
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
        Iterator<String> sessionAndMetric = Splitter.on('.').split(tokens.next()).iterator();
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
  }
}
