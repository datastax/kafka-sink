/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
@CCMConfig(
  config = "authenticator:PasswordAuthenticator",
  jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0"
)
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
        .hasMessageContaining("Provided username cassandra and/or password are incorrect");
  }

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }

  private Map<String, String> makeConnectorProperties(Map<String, String> extras) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("name", "myinstance")
            .put(
                "contactPoints",
                ccm.getInitialContactPoints()
                    .stream()
                    .map(addr -> String.format("%s", addr.getHostAddress()))
                    .collect(Collectors.joining(",")))
            .put("port", String.format("%d", ccm.getBinaryPort()))
            .put("loadBalancing.localDc", "Cassandra")
            .put(
                "topic.mytopic.keyspace",
                session.getKeyspace().orElse(CqlIdentifier.fromInternal("UNKNOWN")).asCql(true))
            .put("topic.mytopic.table", "types")
            .put("topic.mytopic.mapping", "bigintcol=value");

    if (extras != null) {
      builder.putAll(extras);
    }
    return builder.build();
  }
}
