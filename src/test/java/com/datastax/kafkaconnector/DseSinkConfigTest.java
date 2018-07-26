/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkConfig.CONTACT_POINTS_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.DC_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.PORT_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.TABLE_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.TTL_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class DseSinkConfigTest {

  @Test
  void should_error_missing_keyspace() {
    Map<String, String> props = ImmutableMap.<String, String>builder().build();
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"keyspace\"");
  }

  @Test
  void should_error_missing_table() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(KEYSPACE_OPT, "myks").build();
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"table\"");
  }

  @Test
  void should_handle_keyspace_and_table() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "myks")
                .put(TABLE_OPT, "mytable")
                .put(MAPPING_OPT, "c1=f1")
                .build());

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getKeyspace().asInternal()).isEqualTo("myks");
    assertThat(d.getTable().asInternal()).isEqualTo("mytable");

    props.put(KEYSPACE_OPT, "\"myks\"");
    props.put(TABLE_OPT, "\"mytable\"");
    d = new DseSinkConfig(props);
    assertThat(d.getKeyspace().asInternal()).isEqualTo("myks");
    assertThat(d.getTable().asInternal()).isEqualTo("mytable");
  }

  @Test
  void should_handle_case_sensitive_keyspace_and_table() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "MyKs")
                .put(TABLE_OPT, "MyTable")
                .put(MAPPING_OPT, "c1=f1")
                .build());

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getKeyspace().asInternal()).isEqualTo("MyKs");
    assertThat(d.getTable().asInternal()).isEqualTo("MyTable");

    props.put(KEYSPACE_OPT, "\"MyKs\"");
    props.put(TABLE_OPT, "\"MyTable\"");
    d = new DseSinkConfig(props);
    assertThat(d.getKeyspace().asInternal()).isEqualTo("MyKs");
    assertThat(d.getTable().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_error_missing_mapping() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(KEYSPACE_OPT, "myks")
            .put(TABLE_OPT, "mytable")
            .build();
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"mapping\"");
  }

  @Test
  void should_error_invalid_port() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "myks")
                .put(TABLE_OPT, "mytable")
                .put(MAPPING_OPT, "c1=f1")
                .put(PORT_OPT, "foo")
                .build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration port");

    props.put(PORT_OPT, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(PORT_OPT, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  @Test
  void should_error_invalid_ttl() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "myks")
                .put(TABLE_OPT, "mytable")
                .put(MAPPING_OPT, "c1=f1")
                .put(TTL_OPT, "foo")
                .build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration ttl");

    props.put(TTL_OPT, "-2");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least -1");
  }

  @Test
  void should_error_missing_dc_with_contactPoints() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(KEYSPACE_OPT, "myks")
            .put(TABLE_OPT, "mytable")
            .put(MAPPING_OPT, "c1=f1")
            .put(CONTACT_POINTS_OPT, "127.0.0.1")
            .build();
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format("When contact points is provided, %s must also be specified", DC_OPT));
  }

  @Test
  void should_handle_dc_with_contactPoints() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(KEYSPACE_OPT, "myks")
            .put(TABLE_OPT, "mytable")
            .put(MAPPING_OPT, "c1=f1")
            .put(CONTACT_POINTS_OPT, "127.0.0.1, 127.0.1.1")
            .put(DC_OPT, "local")
            .build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getContactPoints()).containsExactly("127.0.0.1", "127.0.1.1");
    assertThat(d.getLocalDc()).isEqualTo("local");
  }

  @Test
  void should_error_bad_mapping() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "myks")
                .put(TABLE_OPT, "mytable")
                .put(MAPPING_OPT, "")
                .build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value '' for configuration mapping: Could not be parsed");

    // Key without value
    props.put(MAPPING_OPT, "jack");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'jack' for configuration mapping:")
        .hasMessageEndingWith("expecting '='");

    props.put(MAPPING_OPT, "jack=");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'jack=' for configuration mapping:");

    // Value without key
    props.put(MAPPING_OPT, "= value");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value '= value' for configuration mapping:");

    // Non-first key without value.
    props.put(MAPPING_OPT, "first = good, jack");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'first = good, jack' for configuration mapping:");

    // Non-first value without key
    props.put(MAPPING_OPT, "first = good, = value");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'first = good, = value' for configuration mapping:");

    // Multiple mappings for the same column
    props.put(MAPPING_OPT, "c1=f1, c1=f2");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'c1=f1, c1=f2' for configuration mapping: Encountered the following errors:")
        .hasMessageContaining("Mapping already defined for column 'c1'");
  }

  @Test
  void should_parse_mapping() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(KEYSPACE_OPT, "myks")
                .put(TABLE_OPT, "mytable")
                .put(
                    MAPPING_OPT,
                    "a=b.c, first = good, \"jack\"=\"bill\",third=great, \"This has spaces, \"\", and commas\" = \"me, \"\" too\"")
                .build());

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getMapping())
        .containsEntry(
            CqlIdentifier.fromInternal("This has spaces, \", and commas"),
            CqlIdentifier.fromInternal("me, \" too"))
        .containsEntry(CqlIdentifier.fromInternal("jack"), CqlIdentifier.fromInternal("bill"))
        .containsEntry(CqlIdentifier.fromInternal("a"), CqlIdentifier.fromInternal("b.c"))
        .containsEntry(CqlIdentifier.fromInternal("first"), CqlIdentifier.fromInternal("good"));
  }
}
