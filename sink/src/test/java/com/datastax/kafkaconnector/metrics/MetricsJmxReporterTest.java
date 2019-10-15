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

import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TableConfigBuilder;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MetricsJmxReporterTest {

  @ParameterizedTest(name = "[{index}] metricNameProvider={0}, expectedMetricName={1}")
  @MethodSource("perTopicKsTableMetricNames")
  void should_create_batch_size_metric_name(
      Function<TableConfig, String> metricNameProvider, String expectedMetricName) {
    // given

    TableConfig tableConfig =
        new TableConfigBuilder("task_1", "ks_1", "table_1", false)
            .addSimpleSetting("mapping", "key=key")
            .build();

    String metricName = metricNameProvider.apply(tableConfig);

    // when
    ObjectName metricDomain =
        MetricsJmxReporter.getObjectName("instance-one", "domain", metricName);

    // then
    assertThat(metricDomain.getCanonicalName())
        .isEqualTo(
            "domain:connector=instance-one,keyspace=ks_1,name="
                + expectedMetricName
                + ",table=table_1,topic=task_1");
  }

  @Test
  void should_create_driver_metrics() {
    // given
    String driverMetric = MetricNamesCreator.createDriverMetricName("s0.cql-client-timeouts");

    // when
    ObjectName batchSizeDomain =
        MetricsJmxReporter.getObjectName("instance-one", "domain", driverMetric);

    // then
    assertThat(batchSizeDomain.getCanonicalName())
        .isEqualTo(
            "domain:connector=instance-one,driver=driver,name=cql-client-timeouts,session=s0");
  }

  @Test
  void should_register_global_failed_record_count_metric() {
    // given
    String recordCountMetric = "globalFailedRecordCount";

    // when
    ObjectName name = MetricsJmxReporter.getObjectName("instance-one", "domain", recordCountMetric);

    // then
    assertThat(name.getCanonicalName())
        .isEqualTo("domain:connector=instance-one,name=globalFailedRecordCount");
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

  private static Stream<? extends Arguments> perTopicKsTableMetricNames() {

    return Stream.of(
        Arguments.of(
            (Function<TableConfig, String>) MetricNamesCreator::createBatchSizeMetricName,
            "batchSize"),
        Arguments.of(
            (Function<TableConfig, String>) MetricNamesCreator::createFailedRecordCountMetricName,
            "failedRecordCount"),
        Arguments.of(
            (Function<TableConfig, String>) MetricNamesCreator::createRecordCountMetricName,
            "recordCount"));
  }
}
