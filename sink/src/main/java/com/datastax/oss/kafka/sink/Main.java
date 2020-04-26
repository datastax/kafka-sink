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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This isn't used in the connector really; it's primarily useful for trying things out and
 * debugging logic in IntelliJ.
 */
public class Main {
  public static void main(String[] args) {
    DseSinkConnector conn = new DseSinkConnector();

    String mappingString = "f1=value.f1, f2=value.f2";
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("mapping", mappingString)
            .put("keyspace", "ks1")
            .put("table", "b")
            .build();
    try {
      conn.start(props);
      String value = "{\"f1\": 42, \"f2\": 96}";
      SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
      DseSinkTask task = new DseSinkTask();
      task.start(props);
      task.put(Collections.singletonList(record));
    } finally {
      conn.stop();
    }
  }
}
