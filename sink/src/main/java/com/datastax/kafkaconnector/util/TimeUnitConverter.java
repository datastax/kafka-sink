/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import java.util.concurrent.TimeUnit;

public class TimeUnitConverter {

  public static long convertToSeconds(TimeUnit timeUnit, Number value) {
    return timeUnit.toSeconds(value.longValue());
  }

  public static long convertToMicroseconds(TimeUnit timestampTimeUnit, Number value) {
    return timestampTimeUnit.toMicros(value.longValue());
  }
}
