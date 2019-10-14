/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.kafkaconnector.config.AuthenticatorConfig.PASSWORD_OPT;
import static com.datastax.kafkaconnector.config.AuthenticatorConfig.PROVIDER_OPT;
import static com.datastax.kafkaconnector.config.AuthenticatorConfig.USERNAME_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
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

  @Test
  void raw_bigint_value() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(USERNAME_OPT, "cassandra")
            .put(PASSWORD_OPT, "cassandra")
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_bad_creds() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(PROVIDER_OPT, "DSE")
            .put(USERNAME_OPT, "cassandra")
            .put(PASSWORD_OPT, "cassandra2")
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining("and/or password are incorrect");
  }
}
