/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

/** Helper methods useful for performing common tasks in *Config classes. */
class ConfigUtil {

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

  /**
   * Convert the given setting value to an absolute path.
   *
   * @param settingValue setting to convert
   * @return the converted path.
   */
  static @Nullable Path getFilePath(@Nullable String settingValue) {
    return settingValue == null || settingValue.isEmpty()
        ? null
        : Paths.get(settingValue).toAbsolutePath().normalize();
  }

  /**
   * Verify that the given file-path exists, is a simple file, and is readable.
   *
   * @param filePath file path to validate
   * @param settingName name of setting whose value is filePath; used in generating error messages
   *     for failures.
   */
  static void assertAccessibleFile(@Nullable Path filePath, String settingName) {
    if (filePath == null) {
      // There's no path to check.
      return;
    }

    if (!Files.exists(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "does not exist");
    }
    if (!Files.isRegularFile(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not a file");
    }
    if (!Files.isReadable(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not readable");
    }
  }
}
