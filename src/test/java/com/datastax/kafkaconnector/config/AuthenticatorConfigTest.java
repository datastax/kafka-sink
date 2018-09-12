/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.AuthenticatorConfig.PASSWORD_OPT;
import static com.datastax.kafkaconnector.config.AuthenticatorConfig.PROVIDER_OPT;
import static com.datastax.kafkaconnector.config.AuthenticatorConfig.USERNAME_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class AuthenticatorConfigTest {
  @Test
  void should_parse_settings() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(USERNAME_OPT, "user1")
            .put(PASSWORD_OPT, "pass1")
            .put(PROVIDER_OPT, "DSE")
            .build();
    AuthenticatorConfig config = new AuthenticatorConfig(props);
    assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.DSE);
    assertThat(config.getUsername()).isEqualTo("user1");
    assertThat(config.getPassword()).isEqualTo("pass1");
  }

  @Test
  void should_coerce_none_provider_when_username_specified() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder().put(USERNAME_OPT, "user1").build();
      AuthenticatorConfig config = new AuthenticatorConfig(props);
      assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.DSE);
    }
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(USERNAME_OPT, "user1")
              .put(PROVIDER_OPT, "None")
              .build();
      AuthenticatorConfig config = new AuthenticatorConfig(props);
      assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.DSE);
    }
  }

  @Test
  void should_not_coerce_explicit_provider_when_username_specified() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(USERNAME_OPT, "user1")
              .put(PROVIDER_OPT, "GSSAPI")
              .build();
      AuthenticatorConfig config = new AuthenticatorConfig(props);
      assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.GSSAPI);
    }
  }

  @Test
  void should_error_invalid_provider() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(PROVIDER_OPT, "foo").build();
    assertThatThrownBy(() -> new AuthenticatorConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Invalid value foo for configuration auth.provider: valid values are None, DSE, GSSAPI");
  }

  @Test
  void should_error_password_without_username() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(PASSWORD_OPT, "pass1").build();
    assertThatThrownBy(() -> new AuthenticatorConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage(String.format("%s was specified without %s", PASSWORD_OPT, USERNAME_OPT));
  }
}
