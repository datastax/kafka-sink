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

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericJsonRecordReconstructor {

  private GenericJsonRecordReconstructor() {}

  private static Logger log = LoggerFactory.getLogger(GenericJsonRecordReconstructor.class);

  private static GenericJsonRecordReconstructor instance;

  public static GenericRecord reconstruct(GenericRecord record) throws Exception {
    if ("org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord"
        .equals(record.getClass().getName())) {
      log.info("reconstruction");
      if (instance == null) instance = new GenericJsonRecordReconstructor();
      return instance.cast(record);
    } else return record;
  }

  private Method getJsonNode;

  private GenericRecord cast(GenericRecord record) throws Exception {
    if (getJsonNode == null) getJsonNode = record.getClass().getMethod("getJsonNode");

    String escaped = getJsonNode.invoke(record).toString();
    String unescaped = StringEscapeUtils.unescapeJava(escaped);
    if (unescaped.startsWith("\"")) unescaped = unescaped.substring(1, unescaped.length() - 1);
    log.info("escaped {}", escaped);
    log.info("unescaped {}", unescaped);
    return new GenericJsonReader(record.getFields())
        .read(unescaped.getBytes(StandardCharsets.UTF_8));
  }
}
