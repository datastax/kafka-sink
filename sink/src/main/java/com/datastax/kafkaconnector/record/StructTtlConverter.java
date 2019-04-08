/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.kafkaconnector.util.TimeUnitConverter;
import java.util.concurrent.TimeUnit;

public class StructTtlConverter {
  public static Number transformField(TimeUnit ttlTimeUnit, Number fieldValue) {
    long resultInSeconds = TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, fieldValue);
    if (fieldValue instanceof Integer) {
      return (int) resultInSeconds;
    } else if (fieldValue instanceof Double) {
      return (double) resultInSeconds;
    } else if (fieldValue instanceof Float) {
      return (float) resultInSeconds;
    }
    return resultInSeconds;
  }
}
