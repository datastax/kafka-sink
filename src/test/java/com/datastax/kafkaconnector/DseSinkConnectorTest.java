/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.config.TopicConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TABLE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TTL_OPT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.metadata.DseMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseColumnMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseKeyspaceMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseTableMetadata;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class DseSinkConnectorTest {
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";

  private static final CqlIdentifier C1_IDENT = CqlIdentifier.fromInternal(C1);
  private static final CqlIdentifier C2_IDENT = CqlIdentifier.fromInternal(C2);
  private static final CqlIdentifier C3_IDENT = CqlIdentifier.fromInternal(C3);

  private DseSession session;
  private DseMetadata metadata;
  private DseKeyspaceMetadata keyspace;
  private DseTableMetadata table;

  @BeforeEach
  void setUp() {
    session = mock(DseSession.class);
    metadata = mock(DseMetadata.class);
    keyspace = mock(DseKeyspaceMetadata.class);
    table = mock(DseTableMetadata.class);
    DseColumnMetadata col1 = mock(DseColumnMetadata.class);
    DseColumnMetadata col2 = mock(DseColumnMetadata.class);
    DseColumnMetadata col3 = mock(DseColumnMetadata.class);
    Map<CqlIdentifier, DseColumnMetadata> columns =
        ImmutableMap.<CqlIdentifier, DseColumnMetadata>builder()
            .put(C1_IDENT, col1)
            .put(C2_IDENT, col2)
            .put(C3_IDENT, col3)
            .build();
    when(session.getMetadata()).thenReturn(metadata);
    when((Optional<DseKeyspaceMetadata>) metadata.getKeyspace(any(CqlIdentifier.class)))
        .thenReturn(Optional.of(keyspace));
    when((Optional<DseTableMetadata>) keyspace.getTable(any(CqlIdentifier.class)))
        .thenReturn(Optional.of(table));
    when((Map<CqlIdentifier, DseColumnMetadata>) table.getColumns()).thenReturn(columns);
    when((Optional<DseColumnMetadata>) table.getColumn(C1_IDENT)).thenReturn(Optional.of(col1));
    when((Optional<DseColumnMetadata>) table.getColumn(C2_IDENT)).thenReturn(Optional.of(col2));
    when((Optional<DseColumnMetadata>) table.getColumn(C3_IDENT)).thenReturn(Optional.of(col3));
    when((List<DseColumnMetadata>) table.getPrimaryKey())
        .thenReturn(Collections.singletonList(col1));
    when(col1.getName()).thenReturn(C1_IDENT);
    when(col2.getName()).thenReturn(C2_IDENT);
    when(col3.getName()).thenReturn(C3_IDENT);
    when(col1.getType()).thenReturn(TEXT);
    when(col2.getType()).thenReturn(TEXT);
    when(col3.getType()).thenReturn(TEXT);
  }

  @Test
  void should_error_that_keyspace_was_not_found() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(Optional.empty());
    when((Optional<DseKeyspaceMetadata>) metadata.getKeyspace("myks"))
        .thenReturn(Optional.of(keyspace));

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("MyKs", "t1", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Invalid value MyKs for configuration topic.mytopic.keyspace: Keyspace does not exist, however a keyspace myks was found. Update the config to use myks if desired.");
  }

  @Test
  void should_error_that_table_was_not_found() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(Optional.empty());
    when((Optional<DseTableMetadata>) keyspace.getTable("mytable")).thenReturn(Optional.of(table));

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("ks1", "MyTable", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Invalid value MyTable for configuration topic.mytopic.table: Table does not exist, however a table mytable was found. Update the config to use mytable if desired.");
  }

  @Test
  void should_error_that_keyspace_was_not_found_2() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(Optional.empty());
    when(metadata.getKeyspace("myks")).thenReturn(Optional.empty());
    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("MyKs", "t1", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Invalid value \"MyKs\" for configuration topic.mytopic.keyspace: Not found");
  }

  @Test
  void should_error_that_table_was_not_found_2() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(Optional.empty());
    when(keyspace.getTable("mytable")).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("ks1", "MyTable", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Invalid value \"MyTable\" for configuration topic.mytopic.table: Not found");
  }

  @Test
  void should_error_when_mapping_does_not_use_primary_key_columns() {
    DseSinkConfig config = makeConfig("myks", "mytable", C3 + "=key.f3");
    assertThatThrownBy(() -> DseSinkConnector.validateMappingColumns(session, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value c3=key.f3 for configuration topic.mytopic.mapping:")
        .hasMessageContaining("but are not mapped: " + C1);
  }

  @Test
  void should_error_when_mapping_has_nonexistent_column() {
    DseSinkConfig config = makeConfig("myks", "mytable", "nocol=key.f3");
    assertThatThrownBy(() -> DseSinkConnector.validateMappingColumns(session, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value nocol=key.f3 for configuration topic.mytopic.mapping:")
        .hasMessageContaining("do not exist in table mytable: nocol");
  }

  @Test
  void should_make_correct_insert_cql() {
    DseSinkConfig config =
        makeConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));
    assertThat(DseSinkConnector.makeInsertStatement(config.getTopicConfigs().get("mytopic")))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) USING TIMESTAMP :%s",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME));
  }

  @Test
  void should_make_correct_insert_cql_with_ttl() {
    DseSinkConfig config =
        makeConfig(
            "myks",
            "mytable",
            String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3),
            1234);
    assertThat(DseSinkConnector.makeInsertStatement(config.getTopicConfigs().get("mytopic")))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) "
                    + "USING TIMESTAMP :%s AND TTL 1234",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME));
  }

  private static DseSinkConfig makeConfig(String keyspaceName, String tableName, String mapping) {
    return makeConfig(keyspaceName, tableName, mapping, -1);
  }

  private static DseSinkConfig makeConfig(
      String keyspaceName, String tableName, String mapping, int ttl) {
    return new DseSinkConfig(
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), keyspaceName)
            .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), tableName)
            .put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), mapping)
            .put(TopicConfig.getTopicSettingName("mytopic", TTL_OPT), String.valueOf(ttl))
            .build());
  }
}
