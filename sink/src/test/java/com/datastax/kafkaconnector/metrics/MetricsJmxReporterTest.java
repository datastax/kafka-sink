/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

class MetricsJmxReporterTest {

  @Test
  void should_create_batch_size_metric_name() {
    // given
    CqlIdentifier keyspace = CqlIdentifier.fromCql("ks_1");
    CqlIdentifier table = CqlIdentifier.fromCql("table_1");
    String batchSizeMetricName =
        MetricNamesCreator.createBatchSizeMetricName("task_1", keyspace, table);

    // when
    ObjectName batchSizeDomain =
        MetricsJmxReporter.getObjectName("instance-one", "domain", batchSizeMetricName);

    // then
    assertThat(batchSizeDomain.getCanonicalName())
        .isEqualTo(
            "domain:connector=instance-one,keyspace=ks_1,name=batchSize,table=table_1,topic=task_1");
  }

  @Test
  void should_create_driver_metrics() {
    // given
    String driverMetric = MetricNamesCreator.createDriverMetricName("0.latency_p99");

    // when
    ObjectName batchSizeDomain =
        MetricsJmxReporter.getObjectName("instance-one", "domain", driverMetric);

    // then
    assertThat(batchSizeDomain.getCanonicalName())
        .isEqualTo("domain:connector=instance-one,driver=driver,name=latency_p99,session=0");
  }

  @Test
  void should_register_record_count_metric() {
    // given
    String recordCountMetric = "recordCount";

    // when
    ObjectName name = MetricsJmxReporter.getObjectName("instance-one", "domain", recordCountMetric);

    // then
    assertThat(name.getCanonicalName()).isEqualTo("domain:connector=instance-one,name=recordCount");
  }

  @Test
  void should_create_hierarchical_metric() {
    // given
    String metricName = "first/second/value";

    // when
    ObjectName name = MetricsJmxReporter.getObjectName("instance-one", "domain", metricName);

    // then
    assertThat(name.getCanonicalName())
        .isEqualTo("domain:connector=instance-one,level1=first,level2=second,name=value");
  }

  @Test
  void should_quote_illegal_characters_from_final_metric_name() {
    // given
    String metricName = "value\"*\\?\n";

    // when
    ObjectName name = MetricsJmxReporter.getObjectName("instance-one", "domain", metricName);

    // then
    assertThat(name.getCanonicalName())
        .isEqualTo("domain:connector=instance-one,name=\"value\\\"\\*\\\\\\?\\n\"");
  }
}
