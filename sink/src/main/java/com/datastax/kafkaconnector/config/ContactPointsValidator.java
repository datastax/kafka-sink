/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;

public class ContactPointsValidator {

  /**
   * This method validates all contact points and throws ConfigException if there is at least one
   * invalid. The correct contact point is proper IPv4 address in a format X.X.X.X or correct domain
   * name according to RFC: http://www.ietf.org/rfc/rfc3490.txt
   *
   * @param contactPoints
   */
  public static void validateContactPoints(List<String> contactPoints) {
    Set<String> invalid =
        contactPoints
            .stream()
            .filter(ContactPointsValidator::isInvalidAddress)
            .collect(Collectors.toSet());
    if (!invalid.isEmpty()) {
      throw new ConfigException(
          String.format(
              "Incorrect %s: %s", DseSinkConfig.CONTACT_POINTS_OPT, String.join(",", invalid)));
    }
  }

  private static boolean isInvalidAddress(String contactPoint) {
    return !InetAddresses.isInetAddress(contactPoint) && !InternetDomainName.isValid(contactPoint);
  }
}
