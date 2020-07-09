/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.sink.config;

import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.KEYTAB_OPT;
import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.PASSWORD_OPT;
import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.PRINCIPAL_OPT;
import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.PROVIDER_OPT;
import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.SERVICE_OPT;
import static com.datastax.oss.kafka.sink.config.AuthenticatorConfig.USERNAME_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.URISyntaxException;
import java.nio.file.Paths;
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
            .put(PROVIDER_OPT, "PLAIN")
            .put(KEYTAB_OPT, "/keytab")
            .put(SERVICE_OPT, "mysvc")
            .put(PRINCIPAL_OPT, "principal")
            .build();
    AuthenticatorConfig config = new AuthenticatorConfig(props);
    assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.PLAIN);
    assertThat(config.getUsername()).isEqualTo("user1");
    assertThat(config.getPassword()).isEqualTo("pass1");
    assertThat(config.getKeyTabPath()).isEqualTo(Paths.get("/keytab").toAbsolutePath().normalize());
    assertThat(config.getService()).isEqualTo("mysvc");
    assertThat(config.getPrincipal()).isEqualTo("principal");
  }

  @Test
  void should_deduce_principal_when_gssapi_and_keytab() throws URISyntaxException {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "GSSAPI")
            .put(
                KEYTAB_OPT,
                Paths.get(getClass().getResource("/cassandra.keytab").toURI())
                    .toAbsolutePath()
                    .normalize()
                    .toString())
            .put(SERVICE_OPT, "mysvc")
            .build();
    AuthenticatorConfig config = new AuthenticatorConfig(props);
    assertThat(config.getPrincipal()).isEqualTo("cassandra@DATASTAX.COM");
  }

  @Test
  void should_respect_principal_when_gssapi_and_keytab() throws URISyntaxException {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "GSSAPI")
            .put(
                KEYTAB_OPT,
                Paths.get(getClass().getResource("/cassandra.keytab").toURI())
                    .toAbsolutePath()
                    .normalize()
                    .toString())
            .put(SERVICE_OPT, "mysvc")
            .put(PRINCIPAL_OPT, "principal")
            .build();
    AuthenticatorConfig config = new AuthenticatorConfig(props);
    assertThat(config.getPrincipal()).isEqualTo("principal");
  }

  @Test
  void should_error_when_missing_service_for_gssapi() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "GSSAPI")
            .put(SERVICE_OPT, "")
            .build();
    assertThatThrownBy(() -> new AuthenticatorConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format("Invalid value <empty> for configuration %s: is required", SERVICE_OPT));
  }

  @Test
  void should_error_when_keytab_not_found_for_gssapi() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "GSSAPI")
            .put(KEYTAB_OPT, "/noexist")
            .build();
    assertThatThrownBy(() -> new AuthenticatorConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format("noexist for configuration %s: does not exist", KEYTAB_OPT));
  }

  @Test
  void should_coerce_none_provider_when_username_specified() {
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder().put(USERNAME_OPT, "user1").build();
      AuthenticatorConfig config = new AuthenticatorConfig(props);
      assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.PLAIN);
    }
    {
      Map<String, String> props =
          ImmutableMap.<String, String>builder()
              .put(USERNAME_OPT, "user1")
              .put(PROVIDER_OPT, "None")
              .build();
      AuthenticatorConfig config = new AuthenticatorConfig(props);
      assertThat(config.getProvider()).isEqualTo(AuthenticatorConfig.Provider.PLAIN);
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
            String.format(
                "Invalid value foo for configuration %s: valid values are None, PLAIN, GSSAPI",
                PROVIDER_OPT));
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
