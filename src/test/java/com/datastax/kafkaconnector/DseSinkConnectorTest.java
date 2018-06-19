/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.TABLE_OPT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DseSinkConnectorTest {
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";

  private DseSession session;
  private Metadata metadata;
  private KeyspaceMetadata keyspace;
  private TableMetadata table;

  @BeforeEach
  void setUp() {
    session = mock(DseSession.class);
    metadata = mock(Metadata.class);
    keyspace = mock(KeyspaceMetadata.class);
    table = mock(TableMetadata.class);
    ColumnMetadata col1 = mock(ColumnMetadata.class);
    ColumnMetadata col2 = mock(ColumnMetadata.class);
    ColumnMetadata col3 = mock(ColumnMetadata.class);
    Map<CqlIdentifier, ColumnMetadata> columns =
        ImmutableMap.<CqlIdentifier, ColumnMetadata>builder()
            .put(CqlIdentifier.fromInternal(C1), col1)
            .put(CqlIdentifier.fromInternal(C2), col2)
            .put(CqlIdentifier.fromInternal(C3), col3)
            .build();
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(keyspace);
    when(keyspace.getTable(anyString())).thenReturn(table);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1)).thenReturn(col1);
    when(table.getColumn(C2)).thenReturn(col2);
    when(table.getColumn(C3)).thenReturn(col3);
    when(table.getPartitionKey()).thenReturn(Collections.singletonList(col1));
    when(col1.getName()).thenReturn(CqlIdentifier.fromInternal(C1));
    when(col2.getName()).thenReturn(CqlIdentifier.fromInternal(C2));
    when(col3.getName()).thenReturn(CqlIdentifier.fromInternal(C3));
    when(col1.getType()).thenReturn(TEXT);
    when(col2.getType()).thenReturn(TEXT);
    when(col3.getType()).thenReturn(TEXT);
  }

  @Test
  void should_error_that_keyspace_was_not_found() {
    when(metadata.getKeyspace("\"MyKs\"")).thenReturn(null);
    when(metadata.getKeyspace("myks")).thenReturn(keyspace);

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("MyKs", "t1", "c1=f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Keyspace does not exist, however a keyspace myks was found. Update the config to use myks if desired.");
  }

  @Test
  void should_error_that_table_was_not_found() {
    when(keyspace.getTable("\"MyTable\"")).thenReturn(null);
    when(keyspace.getTable("mytable")).thenReturn(table);

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("ks1", "MyTable", "c1=f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Table does not exist, however a table mytable was found. Update the config to use mytable if desired.");
  }

  @Test
  void should_error_that_keyspace_was_not_found_2() {
    when(metadata.getKeyspace("\"MyKs\"")).thenReturn(null);
    when(metadata.getKeyspace("myks")).thenReturn(null);
    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("MyKs", "t1", "c1=f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Invalid value \"MyKs\" for configuration keyspace: Not found");
  }

  @Test
  void should_error_that_table_was_not_found_2() {
    when(keyspace.getTable("\"MyTable\"")).thenReturn(null);
    when(keyspace.getTable("mytable")).thenReturn(null);

    assertThatThrownBy(
            () ->
                DseSinkConnector.validateKeyspaceAndTable(
                    session, makeConfig("ks1", "MyTable", "c1=f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Invalid value \"MyTable\" for configuration table: Not found");
  }

  @Test
  void should_error_when_mapping_does_not_use_partition_key_columns() {
    DseSinkConfig config = makeConfig("myks", "mytable", C3 + "=f3");
    assertThatThrownBy(() -> DseSinkConnector.validateMappingColumns(session, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("but are not mapped: " + C1);
  }

  @Test
  void should_error_when_mapping_has_nonexistent_column() {
    DseSinkConfig config = makeConfig("myks", "mytable", "nocol=f3");
    assertThatThrownBy(() -> DseSinkConnector.validateMappingColumns(session, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("do not exist in table mytable: nocol");
  }

  @Test
  void should_make_correct_insert_cql() {
    DseSinkConfig config =
        makeConfig("myks", "mytable", String.format("%s=f1, \"%s\"=f2, %s=f3", C1, C2, C3));
    assertThat(DseSinkConnector.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s)",
                C1, C2, C3, C1, C2, C3));
  }

  private static DseSinkConfig makeConfig(String keyspaceName, String tableName, String mapping) {
    return new DseSinkConfig(
        ImmutableMap.<String, String>builder()
            .put(KEYSPACE_OPT, keyspaceName)
            .put(TABLE_OPT, tableName)
            .put(MAPPING_OPT, mapping)
            .build());
  }
}
