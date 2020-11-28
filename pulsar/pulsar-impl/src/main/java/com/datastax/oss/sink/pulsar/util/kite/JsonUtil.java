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
package com.datastax.oss.sink.pulsar.util.kite;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/** Adopted from https://github.com/kite-sdk */
public class JsonUtil {

  private static final JsonFactory FACTORY = new JsonFactory();

  public static Iterator<JsonNode> parser(final InputStream stream) {
    try {
      JsonParser parser = FACTORY.createParser(stream);
      parser.setCodec(new ObjectMapper());
      return parser.readValuesAs(JsonNode.class);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot read from stream", e);
    }
  }

  public static JsonNode parse(String json) {
    return parse(json, JsonNode.class);
  }

  public static <T> T parse(String json, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(File file) {
    return parse(file, JsonNode.class);
  }

  public static <T> T parse(File file, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(file, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(InputStream in) {
    return parse(in, JsonNode.class);
  }

  public static <T> T parse(InputStream in, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(in, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public abstract static class JsonTreeVisitor<T> {
    protected LinkedList<String> recordLevels = Lists.newLinkedList();

    public T object(ObjectNode object, Map<String, T> fields) {
      return null;
    }

    public T array(ArrayNode array, List<T> elements) {
      return null;
    }

    public T binary(BinaryNode binary) {
      return null;
    }

    public T text(TextNode text) {
      return null;
    }

    public T number(NumericNode number) {
      return null;
    }

    public T bool(BooleanNode bool) {
      return null;
    }

    public T missing(MissingNode missing) {
      return null;
    }

    public T nullNode(NullNode nullNode) {
      return null;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = "BC_UNCONFIRMED_CAST",
    justification = "Uses precondition to validate casts"
  )
  public static <T> T visit(JsonNode node, JsonTreeVisitor<T> visitor) {
    switch (node.getNodeType()) {
      case OBJECT:
        Preconditions.checkArgument(
            node instanceof ObjectNode, "Expected instance of ObjectNode: " + node);

        // use LinkedHashMap to preserve field order
        Map<String, T> fields = Maps.newLinkedHashMap();

        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();

          visitor.recordLevels.push(entry.getKey());
          fields.put(entry.getKey(), visit(entry.getValue(), visitor));
          visitor.recordLevels.pop();
        }

        return visitor.object((ObjectNode) node, fields);

      case ARRAY:
        Preconditions.checkArgument(
            node instanceof ArrayNode, "Expected instance of ArrayNode: " + node);

        List<T> elements = Lists.newArrayListWithExpectedSize(node.size());

        for (JsonNode element : node) {
          elements.add(visit(element, visitor));
        }

        return visitor.array((ArrayNode) node, elements);

      case BINARY:
        Preconditions.checkArgument(
            node instanceof BinaryNode, "Expected instance of BinaryNode: " + node);
        return visitor.binary((BinaryNode) node);

      case STRING:
        Preconditions.checkArgument(
            node instanceof TextNode, "Expected instance of TextNode: " + node);

        return visitor.text((TextNode) node);

      case NUMBER:
        Preconditions.checkArgument(
            node instanceof NumericNode, "Expected instance of NumericNode: " + node);

        return visitor.number((NumericNode) node);

      case BOOLEAN:
        Preconditions.checkArgument(
            node instanceof BooleanNode, "Expected instance of BooleanNode: " + node);

        return visitor.bool((BooleanNode) node);

      case MISSING:
        Preconditions.checkArgument(
            node instanceof MissingNode, "Expected instance of MissingNode: " + node);

        return visitor.missing((MissingNode) node);

      case NULL:
        Preconditions.checkArgument(
            node instanceof NullNode, "Expected instance of NullNode: " + node);

        return visitor.nullNode((NullNode) node);

      default:
        throw new IllegalArgumentException(
            "Unknown node type: " + node.getNodeType() + ": " + node);
    }
  }

  public static Object convertToAvro(GenericData model, JsonNode datum, Schema schema) {
    if (datum == null) {
      return null;
    }
    switch (schema.getType()) {
      case RECORD:
        DatasetRecordException.check(
            datum.isObject(), "Cannot convert non-object to record: %s", datum);
        Object record = model.newRecord(null, schema);
        for (Schema.Field field : schema.getFields()) {
          model.setField(
              record,
              field.name(),
              field.pos(),
              convertField(model, datum.get(field.name()), field));
        }
        return record;

      case MAP:
        DatasetRecordException.check(
            datum.isObject(), "Cannot convert non-object to map: %s", datum);
        Map<String, Object> map = Maps.newLinkedHashMap();
        Iterator<Map.Entry<String, JsonNode>> iter = datum.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();
          map.put(entry.getKey(), convertToAvro(model, entry.getValue(), schema.getValueType()));
        }
        return map;

      case ARRAY:
        DatasetRecordException.check(datum.isArray(), "Cannot convert to array: %s", datum);
        List<Object> list = Lists.newArrayListWithExpectedSize(datum.size());
        for (JsonNode element : datum) {
          list.add(convertToAvro(model, element, schema.getElementType()));
        }
        return list;

      case UNION:
        return convertToAvro(model, datum, resolveUnion(datum, schema.getTypes()));

      case BOOLEAN:
        DatasetRecordException.check(datum.isBoolean(), "Cannot convert to boolean: %s", datum);
        return datum.booleanValue();

      case FLOAT:
        DatasetRecordException.check(
            datum.isFloat() || datum.isInt(), "Cannot convert to float: %s", datum);
        return datum.floatValue();

      case DOUBLE:
        DatasetRecordException.check(
            datum.isDouble() || datum.isFloat() || datum.isLong() || datum.isInt(),
            "Cannot convert to double: %s",
            datum);
        return datum.doubleValue();

      case INT:
        DatasetRecordException.check(datum.isInt(), "Cannot convert to int: %s", datum);
        return datum.intValue();

      case LONG:
        DatasetRecordException.check(
            datum.isLong() || datum.isInt(), "Cannot convert to long: %s", datum);
        return datum.longValue();

      case STRING:
        DatasetRecordException.check(datum.isTextual(), "Cannot convert to string: %s", datum);
        return datum.textValue();

      case ENUM:
        DatasetRecordException.check(datum.isTextual(), "Cannot convert to string: %s", datum);
        return model.createEnum(datum.textValue(), schema);

      case BYTES:
        DatasetRecordException.check(datum.isBinary(), "Cannot convert to binary: %s", datum);
        try {
          return ByteBuffer.wrap(datum.binaryValue());
        } catch (IOException e) {
          throw new DatasetRecordException("Failed to read JSON binary", e);
        }

      case FIXED:
        DatasetRecordException.check(datum.isBinary(), "Cannot convert to fixed: %s", datum);
        byte[] bytes;
        try {
          bytes = datum.binaryValue();
        } catch (IOException e) {
          throw new DatasetRecordException("Failed to read JSON binary", e);
        }
        DatasetRecordException.check(
            bytes.length < schema.getFixedSize(),
            "Binary data is too short: %s bytes for %s",
            bytes.length,
            schema);
        return model.createFixed(null, bytes, schema);

      case NULL:
        return null;

      default:
        // don't use DatasetRecordException because this is a Schema problem
        throw new IllegalArgumentException("Unknown schema type: " + schema);
    }
  }

  private static Object convertField(GenericData model, JsonNode datum, Schema.Field field) {
    try {
      Object value = convertToAvro(model, datum, field.schema());
      if (value != null || nullOk(field.schema())) {
        return value;
      } else {
        return model.getDefaultValue(field);
      }
    } catch (DatasetRecordException e) {
      // add the field name to the error message
      throw new DatasetRecordException(String.format("Cannot convert field %s", field.name()), e);
    } catch (AvroRuntimeException e) {
      throw new DatasetRecordException(
          String.format(
              "Field %s: cannot make %s value: '%s'",
              field.name(), field.schema(), String.valueOf(datum)),
          e);
    }
  }

  private static Schema resolveUnion(JsonNode datum, Collection<Schema> schemas) {
    Set<Schema.Type> primitives = Sets.newHashSet();
    List<Schema> others = Lists.newArrayList();
    for (Schema schema : schemas) {
      if (PRIMITIVES.containsKey(schema.getType())) {
        primitives.add(schema.getType());
      } else {
        others.add(schema);
      }
    }

    // Try to identify specific primitive types
    Schema primitiveSchema = null;
    if (datum == null || datum.isNull()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.NULL);
    } else if (datum.isShort() || datum.isInt()) {
      primitiveSchema =
          closestPrimitive(
              primitives, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE);
    } else if (datum.isLong()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.LONG, Schema.Type.DOUBLE);
    } else if (datum.isFloat()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.FLOAT, Schema.Type.DOUBLE);
    } else if (datum.isDouble()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.DOUBLE);
    } else if (datum.isBoolean()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.BOOLEAN);
    }

    if (primitiveSchema != null) {
      return primitiveSchema;
    }

    // otherwise, select the first schema that matches the datum
    for (Schema schema : others) {
      if (matches(datum, schema)) {
        return schema;
      }
    }

    throw new DatasetRecordException(
        String.format("Cannot resolve union: %s not in %s", datum, schemas));
  }

  // this does not contain string, bytes, or fixed because the datum type
  // doesn't necessarily determine the schema.
  private static ImmutableMap<Schema.Type, Schema> PRIMITIVES =
      ImmutableMap.<Schema.Type, Schema>builder()
          .put(Schema.Type.NULL, Schema.create(Schema.Type.NULL))
          .put(Schema.Type.BOOLEAN, Schema.create(Schema.Type.BOOLEAN))
          .put(Schema.Type.INT, Schema.create(Schema.Type.INT))
          .put(Schema.Type.LONG, Schema.create(Schema.Type.LONG))
          .put(Schema.Type.FLOAT, Schema.create(Schema.Type.FLOAT))
          .put(Schema.Type.DOUBLE, Schema.create(Schema.Type.DOUBLE))
          .build();

  private static Schema closestPrimitive(Set<Schema.Type> possible, Schema.Type... types) {
    for (Schema.Type type : types) {
      if (possible.contains(type) && PRIMITIVES.containsKey(type)) {
        return PRIMITIVES.get(type);
      }
    }
    return null;
  }

  private static boolean matches(JsonNode datum, Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        if (datum.isObject()) {
          // check that each field is present or has a default
          boolean missingField = false;
          for (Schema.Field field : schema.getFields()) {
            if (!datum.has(field.name()) && field.defaultVal() == null) {
              missingField = true;
              break;
            }
          }
          if (!missingField) {
            return true;
          }
        }
        break;
      case UNION:
        if (resolveUnion(datum, schema.getTypes()) != null) {
          return true;
        }
        break;
      case MAP:
        if (datum.isObject()) {
          return true;
        }
        break;
      case ARRAY:
        if (datum.isArray()) {
          return true;
        }
        break;
      case BOOLEAN:
        if (datum.isBoolean()) {
          return true;
        }
        break;
      case FLOAT:
        if (datum.isFloat() || datum.isInt()) {
          return true;
        }
        break;
      case DOUBLE:
        if (datum.isDouble() || datum.isFloat() || datum.isLong() || datum.isInt()) {
          return true;
        }
        break;
      case INT:
        if (datum.isInt()) {
          return true;
        }
        break;
      case LONG:
        if (datum.isLong() || datum.isInt()) {
          return true;
        }
        break;
      case STRING:
        if (datum.isTextual()) {
          return true;
        }
        break;
      case ENUM:
        if (datum.isTextual() && schema.hasEnumSymbol(datum.textValue())) {
          return true;
        }
        break;
      case BYTES:
      case FIXED:
        if (datum.isBinary()) {
          return true;
        }
        break;
      case NULL:
        if (datum == null || datum.isNull()) {
          return true;
        }
        break;
      default: // UNION or unknown
        throw new IllegalArgumentException("Unsupported schema: " + schema);
    }
    return false;
  }

  public static Schema inferSchema(InputStream incoming, final String name, int numRecords) {
    Iterator<Schema> schemas =
        Iterators.transform(
            parser(incoming),
            new Function<JsonNode, Schema>() {
              @Override
              public Schema apply(JsonNode node) {
                return inferSchema(node, name);
              }
            });

    if (!schemas.hasNext()) {
      return null;
    }

    Schema result = schemas.next();
    for (int i = 1; schemas.hasNext() && i < numRecords; i += 1) {
      result = merge(result, schemas.next());
    }

    return result;
  }

  public static Schema inferSchema(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name));
  }

  public static Schema inferSchemaWithMaps(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name).useMaps());
  }

  private static class JsonSchemaVisitor extends JsonTreeVisitor<Schema> {

    private static final Joiner DOT = Joiner.on('.');
    private final String name;
    private boolean objectsToRecords = true;

    public JsonSchemaVisitor(String name) {
      this.name = name;
    }

    public JsonSchemaVisitor useMaps() {
      this.objectsToRecords = false;
      return this;
    }

    @Override
    public Schema object(ObjectNode object, Map<String, Schema> fields) {
      if (objectsToRecords || recordLevels.size() < 1) {
        List<Schema.Field> recordFields = Lists.newArrayListWithExpectedSize(fields.size());

        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
          recordFields.add(
              new Schema.Field(
                  entry.getKey(),
                  entry.getValue(),
                  "Type inferred from '" + object.get(entry.getKey()) + "'",
                  null));
        }

        Schema recordSchema;
        if (recordLevels.size() < 1) {
          recordSchema = Schema.createRecord(name, null, null, false);
        } else {
          recordSchema = Schema.createRecord(DOT.join(recordLevels), null, null, false);
        }

        recordSchema.setFields(recordFields);

        return recordSchema;

      } else {
        // translate to a map; use LinkedHashSet to preserve schema order
        switch (fields.size()) {
          case 0:
            return Schema.createMap(Schema.create(Schema.Type.NULL));
          case 1:
            return Schema.createMap(Iterables.getOnlyElement(fields.values()));
          default:
            return Schema.createMap(mergeOrUnion(fields.values()));
        }
      }
    }

    @Override
    public Schema array(ArrayNode ignored, List<Schema> elementSchemas) {
      // use LinkedHashSet to preserve schema order
      switch (elementSchemas.size()) {
        case 0:
          return Schema.createArray(Schema.create(Schema.Type.NULL));
        case 1:
          return Schema.createArray(Iterables.getOnlyElement(elementSchemas));
        default:
          return Schema.createArray(mergeOrUnion(elementSchemas));
      }
    }

    @Override
    public Schema binary(BinaryNode ignored) {
      return Schema.create(Schema.Type.BYTES);
    }

    @Override
    public Schema text(TextNode ignored) {
      return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema number(NumericNode number) {
      if (number.isInt()) {
        return Schema.create(Schema.Type.INT);
      } else if (number.isLong()) {
        return Schema.create(Schema.Type.LONG);
      } else if (number.isFloat()) {
        return Schema.create(Schema.Type.FLOAT);
      } else if (number.isDouble()) {
        return Schema.create(Schema.Type.DOUBLE);
      } else {
        throw new UnsupportedOperationException(number.getClass().getName() + " is not supported");
      }
    }

    @Override
    public Schema bool(BooleanNode ignored) {
      return Schema.create(Schema.Type.BOOLEAN);
    }

    @Override
    public Schema nullNode(NullNode ignored) {
      return Schema.create(Schema.Type.NULL);
    }

    @Override
    public Schema missing(MissingNode ignored) {
      throw new UnsupportedOperationException("MissingNode is not supported.");
    }
  }

  /**
   * Returns whether null is allowed by the schema.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  public static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Merges two {@link Schema} instances if they are compatible.
   *
   * <p>Two schemas are incompatible if:
   *
   * <ul>
   *   <li>The {@link Schema.Type} does not match.
   *   <li>For record schemas, the record name does not match
   *   <li>For enum schemas, the enum name does not match
   * </ul>
   *
   * <p>Map value and array element types will use unions if necessary, and union schemas are merged
   * recursively.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a merged {@code Schema}
   * @throws IncompatibleSchemaException if the schemas are not compatible
   */
  public static Schema merge(Schema left, Schema right) {
    Schema merged = mergeOnly(left, right);
    IncompatibleSchemaException.check(merged != null, "Cannot merge %s and %s", left, right);
    return merged;
  }

  /**
   * Merges two {@link Schema} instances or returns {@code null}.
   *
   * <p>The two schemas are merged if they are the same type. Records are merged if the two records
   * have the same name or have no names but have a significant number of shared fields.
   *
   * <p>
   *
   * @see {@link #mergeOrUnion} to return a union when a merge is not possible.
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a {@code Schema} for both types
   */
  private static Schema mergeOrUnion(Schema left, Schema right) {
    Schema merged = mergeOnly(left, right);
    if (merged != null) {
      return merged;
    }
    return union(left, right);
  }

  /**
   * Creates a union of two {@link Schema} instances.
   *
   * <p>If either {@code Schema} is a union, this will attempt to merge the other schema with the
   * types contained in that union before adding more types to the union that is produced.
   *
   * <p>If both schemas are not unions, no merge is attempted.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a UNION schema of the to {@code Schema} instances
   */
  private static Schema union(Schema left, Schema right) {
    if (left.getType() == Schema.Type.UNION) {
      if (right.getType() == Schema.Type.UNION) {
        // combine the unions by adding each type in right individually
        Schema combined = left;
        for (Schema type : right.getTypes()) {
          combined = union(combined, type);
        }
        return combined;

      } else {
        boolean notMerged = true;
        // combine a union with a non-union by checking if each type will merge
        List<Schema> types = Lists.newArrayList();
        Iterator<Schema> schemas = left.getTypes().iterator();
        // try to merge each type and stop when one succeeds
        while (schemas.hasNext()) {
          Schema next = schemas.next();
          Schema merged = mergeOnly(next, right);
          if (merged != null) {
            types.add(merged);
            notMerged = false;
            break;
          } else {
            // merge didn't work, add the type
            types.add(next);
          }
        }
        // add the remaining types from the left union
        while (schemas.hasNext()) {
          types.add(schemas.next());
        }

        if (notMerged) {
          types.add(right);
        }

        return Schema.createUnion(types);
      }
    } else if (right.getType() == Schema.Type.UNION) {
      return union(right, left);
    }

    return Schema.createUnion(ImmutableList.of(left, right));
  }

  /**
   * Merges two {@link Schema} instances or returns {@code null}.
   *
   * <p>The two schemas are merged if they are the same type. Records are merged if the two records
   * have the same name or have no names but have a significant number of shared fields.
   *
   * <p>
   *
   * @see {@link #mergeOrUnion} to return a union when a merge is not possible.
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a merged {@code Schema} or {@code null} if merging is not possible
   */
  private static Schema mergeOnly(Schema left, Schema right) {
    if (Objects.equal(left, right)) {
      return left;
    }

    // handle primitive type promotion; doesn't promote integers to floats
    switch (left.getType()) {
      case INT:
        if (right.getType() == Schema.Type.LONG) {
          return right;
        }
        break;
      case LONG:
        if (right.getType() == Schema.Type.INT) {
          return left;
        }
        break;
      case FLOAT:
        if (right.getType() == Schema.Type.DOUBLE) {
          return right;
        }
        break;
      case DOUBLE:
        if (right.getType() == Schema.Type.FLOAT) {
          return left;
        }
    }

    // any other cases where the types don't match must be combined by a union
    if (left.getType() != right.getType()) {
      return null;
    }

    switch (left.getType()) {
      case UNION:
        return union(left, right);
      case RECORD:
        if (left.getName() == null
            && right.getName() == null
            && fieldSimilarity(left, right) < SIMILARITY_THRESH) {
          return null;
        } else if (!Objects.equal(left.getName(), right.getName())) {
          return null;
        }

        Schema combinedRecord =
            Schema.createRecord(
                coalesce(left.getName(), right.getName()),
                coalesce(left.getDoc(), right.getDoc()),
                coalesce(left.getNamespace(), right.getNamespace()),
                false);
        combinedRecord.setFields(mergeFields(left, right));

        return combinedRecord;

      case MAP:
        return Schema.createMap(mergeOrUnion(left.getValueType(), right.getValueType()));

      case ARRAY:
        return Schema.createArray(mergeOrUnion(left.getElementType(), right.getElementType()));

      case ENUM:
        if (!Objects.equal(left.getName(), right.getName())) {
          return null;
        }
        Set<String> symbols = Sets.newLinkedHashSet();
        symbols.addAll(left.getEnumSymbols());
        symbols.addAll(right.getEnumSymbols());
        return Schema.createEnum(
            left.getName(),
            coalesce(left.getDoc(), right.getDoc()),
            coalesce(left.getNamespace(), right.getNamespace()),
            ImmutableList.copyOf(symbols));

      default:
        // all primitives are handled before the switch by the equality check.
        // schemas that reach this point are not primitives and also not any of
        // the above known types.
        throw new UnsupportedOperationException("Unknown schema type: " + left.getType());
    }
  }
  /** Returns the first non-null object that is passed in. */
  private static <E> E coalesce(E... objects) {
    for (E object : objects) {
      if (object != null) {
        return object;
      }
    }
    return null;
  }

  /**
   * Merges {@link Schema} instances and creates a union of schemas if any are incompatible.
   *
   * <p>Schemas are incompatible if:
   *
   * <ul>
   *   <li>The {@link Schema.Type} does not match.
   *   <li>For record schemas, the record name does not match
   *   <li>For enum schemas, the enum name does not match
   * </ul>
   *
   * <p>Map value, array element, and record field types types will use unions if necessary, and
   * union schemas are merged recursively.
   *
   * @param schemas a set of {@code Schema} instances to merge
   * @return a combined {@code Schema}
   */
  public static Schema mergeOrUnion(Iterable<Schema> schemas) {
    Iterator<Schema> iter = schemas.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    Schema result = iter.next();
    while (iter.hasNext()) {
      result = mergeOrUnion(result, iter.next());
    }
    return result;
  }

  private static float fieldSimilarity(Schema left, Schema right) {
    // check whether the unnamed records appear to be the same record
    Set<String> leftNames = names(left.getFields());
    Set<String> rightNames = names(right.getFields());
    int common = Sets.intersection(leftNames, rightNames).size();
    float leftRatio = ((float) common) / ((float) leftNames.size());
    float rightRatio = ((float) common) / ((float) rightNames.size());
    return hmean(leftRatio, rightRatio);
  }

  private static Set<String> names(Collection<Schema.Field> fields) {
    Set<String> names = Sets.newHashSet();
    for (Schema.Field field : fields) {
      names.add(field.name());
    }
    return names;
  }

  private static float SIMILARITY_THRESH = 0.3f;

  private static float hmean(float left, float right) {
    return (2.0f * left * right) / (left + right);
  }

  private static List<Schema.Field> mergeFields(Schema left, Schema right) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field leftField : left.getFields()) {
      Schema.Field rightField = right.getField(leftField.name());
      if (rightField != null) {
        fields.add(
            new Schema.Field(
                leftField.name(),
                mergeOrUnion(leftField.schema(), rightField.schema()),
                coalesce(leftField.doc(), rightField.doc()),
                coalesce(leftField.defaultVal(), rightField.defaultVal())));
      } else {
        if (leftField.defaultVal() != null) {
          fields.add(copy(leftField));
        } else {
          fields.add(
              new Schema.Field(
                  leftField.name(),
                  nullableForDefault(leftField.schema()),
                  leftField.doc(),
                  NULL_DEFAULT));
        }
      }
    }

    for (Schema.Field rightField : right.getFields()) {
      if (left.getField(rightField.name()) == null) {
        if (rightField.defaultVal() != null) {
          fields.add(copy(rightField));
        } else {
          fields.add(
              new Schema.Field(
                  rightField.name(),
                  nullableForDefault(rightField.schema()),
                  rightField.doc(),
                  NULL_DEFAULT));
        }
      }
    }

    return fields;
  }
  /**
   * Creates a new field with the same name, schema, doc, and default value as the incoming schema.
   *
   * <p>Fields cannot be used in more than one record (not Immutable?).
   */
  public static Schema.Field copy(Schema.Field field) {
    return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
  }
  /**
   * Returns a union {@link Schema} of NULL and the given {@code schema}.
   *
   * <p>A NULL schema is always the first type in the union so that a null default value can be set.
   *
   * @param schema a {@code Schema}
   * @return a union of null and the given schema
   */
  private static Schema nullableForDefault(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return schema;
    }

    if (schema.getType() != Schema.Type.UNION) {
      return Schema.createUnion(ImmutableList.of(NULL, schema));
    }

    if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      return schema;
    }

    List<Schema> types = Lists.newArrayList();
    types.add(NULL);
    for (Schema type : schema.getTypes()) {
      if (type.getType() != Schema.Type.NULL) {
        types.add(type);
      }
    }

    return Schema.createUnion(types);
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private static final NullNode NULL_DEFAULT = NullNode.getInstance();
}
