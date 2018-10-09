/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.util.StringUtil.singleQuote;

import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.schema.MappingBaseVisitor;
import com.datastax.kafkaconnector.schema.MappingLexer;
import com.datastax.kafkaconnector.schema.MappingParser;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.kafka.common.config.ConfigException;

/** Processor for a mapping string. */
class MappingInspector extends MappingBaseVisitor<CqlIdentifier> {

  // A mapping spec may refer to these special variables which are used to bind
  // input fields to the write timestamp or ttl of the record.
  // NB: This logic isn't currently used in the Kafka connector, but it might be
  // some day...

  private static final String INTERNAL_TTL_VARNAME = "kafka_internal_ttl";

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  private Map<CqlIdentifier, CqlIdentifier> mapping;
  private List<String> errors;

  public MappingInspector(String mapping, String settingName) {
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
                settingName,
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
    String fieldString = field.asInternal();
    if (fieldString.equals("value") || fieldString.equals("key")) {
      field = CqlIdentifier.fromInternal(fieldString + '.' + RawData.FIELD_NAME);
    } else if (!fieldString.startsWith("key.") && !fieldString.startsWith("value.")) {
      errors.add(
          String.format(
              "Invalid field name '%s': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.'.",
              fieldString));
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
        column = SinkUtil.TIMESTAMP_VARNAME;
      }
      return CqlIdentifier.fromInternal(column);
    }
    return CqlIdentifier.fromCql(column);
  }
}
