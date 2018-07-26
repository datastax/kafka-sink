/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.util.StringUtil.singleQuote;

import com.datastax.kafkaconnector.schema.MappingBaseVisitor;
import com.datastax.kafkaconnector.schema.MappingLexer;
import com.datastax.kafkaconnector.schema.MappingParser;
import com.datastax.kafkaconnector.util.StringUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class DseSinkConfig extends AbstractConfig {
  static final String KEYSPACE_OPT = "keyspace";
  static final String TABLE_OPT = "table";
  static final String CONTACT_POINTS_OPT = "contactPoints";
  static final String PORT_OPT = "port";
  static final String DC_OPT = "loadBalancing.localDc";
  static final String MAPPING_OPT = "mapping";
  static final String TTL_OPT = "ttl";

  static ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KEYSPACE_OPT,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Keyspace to which to load messages")
          .define(
              TABLE_OPT,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Table to which to load messages")
          .define(
              MAPPING_OPT,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Mapping of record fields to dse columns, in the form of 'col1=value.f1, col2=key.f1'")
          .define(
              CONTACT_POINTS_OPT,
              ConfigDef.Type.LIST,
              Collections.EMPTY_LIST,
              ConfigDef.Importance.HIGH,
              "Initial DSE node contact points")
          .define(
              PORT_OPT,
              ConfigDef.Type.INT,
              9042,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.HIGH,
              "Port to connect to on DSE nodes")
          .define(
              DC_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The datacenter name (commonly dc1, dc2, etc.) local to the machine on which the connector is running.")
          .define(
              TTL_OPT,
              ConfigDef.Type.INT,
              -1,
              ConfigDef.Range.atLeast(-1),
              ConfigDef.Importance.HIGH,
              "TTL of rows inserted in DSE nodes");

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final int port;
  private final List<String> contactPoints;
  private final String localDc;
  private final String mappingString;
  private final Map<CqlIdentifier, CqlIdentifier> mapping;
  private final int ttl;

  DseSinkConfig(final Map<?, ?> settings) {
    super(CONFIG_DEF, settings, false);

    keyspace = parseLoosely(getString(KEYSPACE_OPT));
    table = parseLoosely(getString(TABLE_OPT));
    port = getInt(PORT_OPT);
    contactPoints = getList(CONTACT_POINTS_OPT);
    localDc = getString(DC_OPT);
    mappingString = getString(MAPPING_OPT);
    mapping = parseMappingString(mappingString);
    ttl = getInt(TTL_OPT);

    // Verify that if contact-points are provided, local dc
    // is also specified.
    if (!contactPoints.isEmpty() && StringUtil.isEmpty(localDc)) {
      throw new ConfigException(
          CONTACT_POINTS_OPT,
          contactPoints,
          String.format("When contact points is provided, %s must also be specified", DC_OPT));
    }
  }

  static Map<CqlIdentifier, CqlIdentifier> parseMappingString(String mappingString) {
    MappingInspector inspector = new MappingInspector(mappingString);
    List<String> errors = inspector.getErrors();
    if (!errors.isEmpty()) {
      throw new ConfigException(
          MAPPING_OPT,
          singleQuote(mappingString),
          String.format(
              "Encountered the following errors:%n%s",
              errors.stream().collect(Collectors.joining(String.format("%n  ")))));
    }

    return inspector.getMapping();
  }

  CqlIdentifier getKeyspace() {
    return keyspace;
  }

  CqlIdentifier getTable() {
    return table;
  }

  int getPort() {
    return port;
  }

  Map<CqlIdentifier, CqlIdentifier> getMapping() {
    return mapping;
  }

  String getMappingString() {
    return mappingString;
  }

  List<String> getContactPoints() {
    return contactPoints;
  }

  String getLocalDc() {
    return localDc;
  }

  int getTtl() {
    return ttl;
  }

  @Override
  public String toString() {
    return String.format(
        "Configuration options:%n"
            + "        keyspace = %s%n"
            + "        table = %s%n"
            + "        contactPoints = %s%n"
            + "        port = %d%n"
            + "        localDc = %s%n",
        keyspace, table, contactPoints, port, localDc);
  }

  private static CqlIdentifier parseLoosely(String value) {
    // If the value is unquoted, treat it as a literal (no real parsing).
    // Otherwise parse it as cql. The idea is that users should be able to specify
    // case-sensitive identifiers in the mapping spec and config properties.

    return value.startsWith("\"")
        ? CqlIdentifier.fromCql(value)
        : CqlIdentifier.fromInternal(value);
  }

  static class MappingInspector extends MappingBaseVisitor<CqlIdentifier> {

    // A mapping spec may refer to these special variables which are used to bind
    // input fields to the write timestamp or ttl of the record.

    static final String INTERNAL_TTL_VARNAME = "kafka_internal_ttl";
    static final String INTERNAL_TIMESTAMP_VARNAME = "kafka_internal_timestamp";

    private static final String EXTERNAL_TTL_VARNAME = "__ttl";
    private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

    private Map<CqlIdentifier, CqlIdentifier> mapping;
    private List<String> errors;

    public MappingInspector(String mapping) {
      CodePointCharStream input = CharStreams.fromString(mapping);
      MappingLexer lexer = new MappingLexer(input);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      MappingParser parser = new MappingParser(tokens);
      BaseErrorListener listener =
          new BaseErrorListener() {

            @Override
            public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int col,
                String msg,
                RecognitionException e) {
              throw new ConfigException(
                  MAPPING_OPT,
                  singleQuote(mapping),
                  String.format("Could not be parsed at line %d:%d: %s", line, col, msg));
            }
          };
      lexer.removeErrorListeners();
      lexer.addErrorListener(listener);
      parser.removeErrorListeners();
      parser.addErrorListener(listener);
      MappingParser.MappingContext ctx = parser.mapping();

      this.mapping = new LinkedHashMap<>();
      errors = new ArrayList<>();
      visit(ctx);
    }

    public Map<CqlIdentifier, CqlIdentifier> getMapping() {
      return mapping;
    }

    List<String> getErrors() {
      return errors;
    }

    @Override
    public CqlIdentifier visitMapping(MappingParser.MappingContext ctx) {
      if (!ctx.mappedEntry().isEmpty()) {
        for (MappingParser.MappedEntryContext entry : ctx.mappedEntry()) {
          visitMappedEntry(entry);
        }
      }
      return null;
    }

    @Override
    public CqlIdentifier visitMappedEntry(MappingParser.MappedEntryContext ctx) {
      CqlIdentifier field = visitField(ctx.field());
      CqlIdentifier column = visitColumn(ctx.column());
      if (mapping.containsKey(column)) {
        errors.add(String.format("Mapping already defined for column '%s'", column.asInternal()));
      }
      mapping.put(column, field);
      return null;
    }

    @Override
    public CqlIdentifier visitField(MappingParser.FieldContext ctx) {
      String field = ctx.getText();

      // If the field name is unquoted, treat it as a literal (no real parsing).
      // Otherwise parse it as cql. The idea is that users should be able to specify
      // case-sensitive identifiers in the mapping spec.

      return ctx.QUOTED_STRING() == null
          ? CqlIdentifier.fromInternal(field)
          : CqlIdentifier.fromCql(field);
    }

    @Override
    public CqlIdentifier visitColumn(MappingParser.ColumnContext ctx) {
      String column = ctx.getText();

      // If the column name is unquoted, treat it as a literal (no real parsing).
      // Otherwise parse it as cql. The idea is that users should be able to specify
      // case-sensitive identifiers in the mapping spec.

      if (ctx.QUOTED_STRING() == null) {
        // Rename the user-specified __ttl and __timestamp vars to the (legal) bound variable
        // names.
        if (column.equals(EXTERNAL_TTL_VARNAME)) {
          column = INTERNAL_TTL_VARNAME;
        } else if (column.equals(EXTERNAL_TIMESTAMP_VARNAME)) {
          column = INTERNAL_TIMESTAMP_VARNAME;
        }
        return CqlIdentifier.fromInternal(column);
      }
      return CqlIdentifier.fromCql(column);
    }
  }
}
