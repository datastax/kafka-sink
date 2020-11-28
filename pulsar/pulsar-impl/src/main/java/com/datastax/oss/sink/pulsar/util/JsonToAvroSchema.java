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
package com.datastax.oss.sink.pulsar.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToAvroSchema {

  private static final Logger logger = LoggerFactory.getLogger(JsonToAvroSchema.class);

  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String ARRAY = "array";
  private static final String ITEMS = "items";
  private static final String STRING = "string";
  private static final String RECORD = "record";
  private static final String FIELDS = "fields";
  private static final String NULL = "null";
  private static final String BOOLEAN = "boolean";

  private final ObjectMapper mapper;

  public JsonToAvroSchema(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public Schema infer(String json, String name, String namespace) throws IOException {
    JsonNode node = mapper.readTree(json);
    if (!node.isObject() && !node.isArray())
      throw new IllegalArgumentException("json is not a struct " + json);
    return infer(node, name, namespace);
  }

  public Schema infer(JsonNode root, String name, String namespace) throws IOException {
    ObjectNode finalSchema = mapper.createObjectNode();
    finalSchema.put("namespace", namespace);
    finalSchema.put(NAME, name);
    if (root.isArray()) {
      finalSchema.put(TYPE, ARRAY);
      Object elementType = elementType(root.get(0), new AtomicInteger());
      if (elementType instanceof String) finalSchema.put("items", (String) elementType);
      else finalSchema.set("items", (JsonNode) elementType);
    } else {
      finalSchema.put(TYPE, RECORD);
      finalSchema.set(FIELDS, getFields(root, new AtomicInteger()));
    }
    return new Schema.Parser().parse(mapper.writeValueAsString(finalSchema));
  }

  private String numberType(JsonNode node) {
    String type = "double";
    if (node.isShort() || node.isInt()) type = "int";
    else if (node.isLong()) type = "long";
    else if (node.isFloat()) type = "float";
    return type;
  }

  private ArrayNode getFields(JsonNode jsonNode, AtomicInteger nestedCounter) {
    ArrayNode fields = mapper.createArrayNode();
    Iterator<Map.Entry<String, JsonNode>> elements = jsonNode.fields();

    Map.Entry<String, JsonNode> map;
    while (elements.hasNext()) {
      map = elements.next();
      JsonNode nextNode = map.getValue();

      switch (nextNode.getNodeType()) {
        case NUMBER:
          fields.add(
              mapper.createObjectNode().put(NAME, map.getKey()).put(TYPE, numberType(nextNode)));
          break;

        case STRING:
          fields.add(mapper.createObjectNode().put(NAME, map.getKey()).put(TYPE, STRING));
          break;

        case ARRAY:
          ArrayNode arrayNode = (ArrayNode) nextNode;
          JsonNode element = arrayNode.get(0);
          ObjectNode objectNode = mapper.createObjectNode();
          objectNode.put(NAME, map.getKey());

          if (element == null) {
            break;
          }

          if (element.getNodeType() == JsonNodeType.NUMBER) {
            objectNode.set(
                TYPE, mapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, numberType(element)));
            fields.add(objectNode);
          } else if (element.getNodeType() == JsonNodeType.STRING) {
            objectNode.set(TYPE, mapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, STRING));
            fields.add(objectNode);
          } else {
            objectNode.set(
                TYPE,
                mapper
                    .createObjectNode()
                    .put(TYPE, ARRAY)
                    .set(
                        ITEMS,
                        mapper
                            .createObjectNode()
                            .put(TYPE, RECORD)
                            .put(NAME, "nested" + nestedCounter.incrementAndGet())
                            .set(FIELDS, getFields(element, nestedCounter))));
            fields.add(objectNode);
          }
          break;

        case OBJECT:
          ObjectNode node = mapper.createObjectNode();
          node.put(NAME, map.getKey());
          node.set(
              TYPE,
              mapper
                  .createObjectNode()
                  .put(TYPE, RECORD)
                  .put(NAME, "nested" + nestedCounter.incrementAndGet())
                  .set(FIELDS, getFields(nextNode, nestedCounter)));
          fields.add(node);
          break;

        case NULL:
          ObjectNode unionNullNode = mapper.createObjectNode();
          unionNullNode.put(NAME, map.getKey());
          unionNullNode.putArray(TYPE).add(NULL);
          fields.add(unionNullNode);
          break;

        case BOOLEAN:
          fields.add(mapper.createObjectNode().put(NAME, map.getKey()).put(TYPE, BOOLEAN));
          break;

        default:
          logger.error("Node type not found - " + nextNode.getNodeType());
          throw new RuntimeException(
              "Unable to determine action for nodetype "
                  + nextNode.getNodeType()
                  + "; Allowed types are ARRAY, STRING, NUMBER, OBJECT");
      }
    }
    return fields;
  }

  private Object elementType(JsonNode element, AtomicInteger nestedCounter) {
    if (element.getNodeType() == JsonNodeType.NUMBER) {
      return numberType(element);
    } else if (element.getNodeType() == JsonNodeType.STRING) {
      return STRING;
    } else {
      return mapper
          .createObjectNode()
          .put(TYPE, RECORD)
          .put(NAME, "nested" + nestedCounter.incrementAndGet())
          .set(FIELDS, getFields(element, nestedCounter));
    }
  }

  //    private void buildArray(JsonNode element, ObjectNode schemaNode, AtomicInteger
  // nestedCounter) {
  //        if (element.getNodeType() == JsonNodeType.NUMBER) {
  //            schemaNode.put(TYPE, ARRAY).put(ITEMS, numberType(element));
  //        } else if (element.getNodeType() == JsonNodeType.STRING) {
  //            schemaNode.put(TYPE, ARRAY).put(ITEMS, STRING);
  //        } else {
  //            schemaNode.put(TYPE, ARRAY).set(ITEMS, mapper.createObjectNode()
  //                    .put(TYPE, RECORD).put(NAME, "nested" +
  // nestedCounter.incrementAndGet()).set(FIELDS,
  //                            getFields(element, nestedCounter)));
  //        }
  //    }
  //
}
