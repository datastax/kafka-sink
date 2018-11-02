/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

/** Utility class to house useful methods and constants that the rest of the application may use. */
public class SinkUtil {
  public static final String TIMESTAMP_VARNAME = "kafka_internal_timestamp";
  public static final String NAME_OPT = "name";

  /** This is a utility class and should never be instantiated. */
  private SinkUtil() {}
}
