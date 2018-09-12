/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.ConfigUtil.configToString;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Topic-specific connector configuration. */
public class AuthenticatorConfig extends AbstractConfig {
  public static final String PROVIDER_OPT = "auth.provider";
  public static final String USERNAME_OPT = "auth.username";
  public static final String PASSWORD_OPT = "auth.password";

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              PROVIDER_OPT,
              ConfigDef.Type.STRING,
              "None",
              ConfigDef.Importance.HIGH,
              "None | DSE | GSSAPI")
          .define(
              USERNAME_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Username for DSE provider authentication")
          .define(
              PASSWORD_OPT,
              ConfigDef.Type.PASSWORD,
              "",
              ConfigDef.Importance.HIGH,
              "Password for DSE provider authentication");

  AuthenticatorConfig(Map<String, String> authSettings) {
    super(CONFIG_DEF, sanitizeAuthSettings(authSettings), false);

    // Verify that the provider value is valid.
    getProvider();

    // If password is specified, username must be.
    if (!getPassword().isEmpty() && getUsername().isEmpty()) {
      throw new ConfigException(
          String.format("%s was specified without %s", PASSWORD_OPT, USERNAME_OPT));
    }
  }

  private static Map<String, String> sanitizeAuthSettings(Map<String, String> authSettings) {
    if ((authSettings.containsKey(USERNAME_OPT) || authSettings.containsKey(PASSWORD_OPT))
        && ("None".equals(authSettings.get(PROVIDER_OPT))
            || authSettings.get(PROVIDER_OPT) == null)) {
      // Username/password was provided. Coerce the provider type to DSE.
      Map<String, String> mutated = new HashMap<>(authSettings);
      mutated.put(PROVIDER_OPT, "DSE");

      return ImmutableMap.<String, String>builder().putAll(mutated).build();
    }
    return authSettings;
  }

  public Provider getProvider() {
    String providerString = getString(PROVIDER_OPT);
    try {
      return Provider.valueOf(providerString);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(PROVIDER_OPT, providerString, "valid values are None, DSE, GSSAPI");
    }
  }

  public String getPassword() {
    return getPassword(PASSWORD_OPT).value();
  }

  public String getUsername() {
    return getString(USERNAME_OPT);
  }

  @Override
  public String toString() {
    return configToString(this, "auth.", PROVIDER_OPT, USERNAME_OPT, PASSWORD_OPT);
  }

  public enum Provider {
    None,
    DSE,
    GSSAPI
  }
}
