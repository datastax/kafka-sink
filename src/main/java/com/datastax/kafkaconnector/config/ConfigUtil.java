/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;

/** Helper methods useful for performing common tasks in *Config classes. */
public class ConfigUtil {
  /** This is a utility class; no one should instantiate it. */
  private ConfigUtil() {}

  /**
   * Produce a formatted string of the given settings in the given config object.
   *
   * @param config object containing settings to extract.
   * @param prefixToExcise setting name prefix to remove when emitting setting names (e.g. "auth."
   *     or "ssl.").
   * @param settingNames names of settings to extract.
   * @return lines of the form "setting: value".
   */
  static String configToString(
      AbstractConfig config, String prefixToExcise, String... settingNames) {
    return Arrays.stream(settingNames)
        .map(
            s ->
                String.format(
                    "%s: %s",
                    s.substring(prefixToExcise.length()), config.values().get(s).toString()))
        .collect(Collectors.joining("\n"));
  }
}
