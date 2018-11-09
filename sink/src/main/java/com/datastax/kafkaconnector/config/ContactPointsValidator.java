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
   * invalid. A valid contact point is either a valid IP address in its canonical form, or a valid
   * domain name.
   *
   * @param contactPoints - list of contact points to validate
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
