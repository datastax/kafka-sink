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
package com.datastax.oss.pulsar.sink.ccm.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.AuthenticatorConfig.Provider;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.pulsar.sink.ccm.EndToEndCCMITBase;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@CCMConfig(
  config = "authenticator:PasswordAuthenticator",
  jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0"
)
@Tag("medium")
class PlaintextAuthCCMIT extends EndToEndCCMITBase {
  public PlaintextAuthCCMIT(
      CCMCluster ccm, @SessionConfig(credentials = {"cassandra", "cassandra"}) CqlSession session) {
    super(ccm, session);
  }

  @ParameterizedTest(name = "[{index}] extras={0}")
  @MethodSource("correctCredentialsProvider")
  void should_insert_successfully_with_correct_credentials(Map<String, String> extras) {
    taskConfigs.add(makeConnectorProperties(extras));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            null,
            new GenericRecordImpl().put("bigint", 5725368L),
            recordType);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);

    // auth.provider was coerced to the database
    assertThat(task.getInstanceState().getConfig().getAuthenticatorConfig().getProvider())
        .isEqualTo(Provider.PLAIN);
  }

  @ParameterizedTest(name = "[{index}] extras={0}")
  @MethodSource("incorrectCredentialsProvider")
  void should_error_that_password_or_username_is_incorrect(Map<String, String> extras) {
    taskConfigs.add(makeConnectorProperties(extras));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", null, new GenericRecordImpl(), recordType);
    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining("and/or password are incorrect");
  }

  private static Stream<? extends Arguments> incorrectCredentialsProvider() {
    ImmutableMap<String, String> incorrectCredentials =
        ImmutableMap.<String, String>builder()
            .put(AuthenticatorConfig.USERNAME_OPT, "cassandra")
            .put(AuthenticatorConfig.PASSWORD_OPT, "cassandra2")
            .build();

    return Stream.of(
        Arguments.of(
            ImmutableMap.builder()
                .putAll(incorrectCredentials)
                .put(AuthenticatorConfig.PROVIDER_OPT, "PLAIN")
                .build()),
        Arguments.of(
            ImmutableMap.builder()
                .putAll(incorrectCredentials)
                .put(AuthenticatorConfig.PROVIDER_OPT, "None")
                .build()), // should infer auth.provider to PLAIN
        Arguments.of(incorrectCredentials) // should infer auth.provider to PLAIN
        );
  }

  private static Stream<? extends Arguments> correctCredentialsProvider() {
    ImmutableMap<String, String> incorrectCredentials =
        ImmutableMap.<String, String>builder()
            .put(AuthenticatorConfig.USERNAME_OPT, "cassandra")
            .put(AuthenticatorConfig.PASSWORD_OPT, "cassandra")
            .build();

    return Stream.of(
        Arguments.of(
            ImmutableMap.builder()
                .putAll(incorrectCredentials)
                .put(AuthenticatorConfig.PROVIDER_OPT, "PLAIN")
                .build()),
        Arguments.of(
            ImmutableMap.builder()
                .putAll(incorrectCredentials)
                .put(AuthenticatorConfig.PROVIDER_OPT, "None")
                .build()), // should infer auth.provider to PLAIN
        Arguments.of(incorrectCredentials) // should infer auth.provider to PLAIN
        );
  }
}
