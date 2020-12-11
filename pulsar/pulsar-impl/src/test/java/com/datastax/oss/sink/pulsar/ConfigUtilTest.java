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
package com.datastax.oss.sink.pulsar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigUtilTest {

  private Map<String, Object> map =
      ImmutableMap.<String, Object>builder()
          .put("node1", ImmutableMap.builder().put("val1", 1).put("val2", "string").build())
          .put(
              "node2",
              ImmutableMap.builder()
                  .put(
                      "sub21",
                      ImmutableMap.builder().put("subval211", true).put("subval212", 1.2d).build())
                  .put("sub2", "string2")
                  .build())
          .build();

  @Test
  void flat() {
    Map<String, Object> flat = ConfigUtil.flat(map);
    assertEquals(5, flat.size());
    assertEquals(1, flat.get("node1.val1"));
    assertEquals("string", flat.get("node1.val2"));
    assertEquals(true, flat.get("node2.sub21.subval211"));
    assertEquals(1.2d, flat.get("node2.sub21.subval212"));
    assertEquals("string2", flat.get("node2.sub2"));
  }

  @Test
  void tree() {
    Map<String, Object> flat = ConfigUtil.flat(map);
    Map<String, Object> tree = ConfigUtil.tree(flat);
    assertEquals(map, tree);
  }

  @Test
  void update2() throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    //noinspection unchecked
    Map<String, Object> read =
        mapper.readValue(getClass().getResource("/config-example.yaml"), Map.class);
    assertEquals("192.168.1.103", read.get("contactPoints"));
    assertEquals("http://localhost:8080", ConfigUtil.value(read, "pulsarAdminService.url"));
    read =
        ConfigUtil.extend(
            read,
            ImmutableMap.<String, Object>builder()
                .put("pulsarAdminService.url", "addvalue")
                .put("contactPoints", "contacts")
                .build());
    assertEquals("addvalue", ConfigUtil.value(read, "pulsarAdminService.url"));
    assertEquals("contacts", read.get("contactPoints"));
  }

  @Test
  void value() {
    assertEquals(1.2d, ConfigUtil.value(map, "node2.sub21.subval212"));
    assertEquals("string2", ConfigUtil.value(map, "node2.sub2"));
  }

  @Test
  void update() {
    Map<String, Object> update =
        ImmutableMap.<String, Object>builder()
            .put(
                "node1",
                ImmutableMap.builder()
                    .put(
                        "sub11",
                        ImmutableMap.builder()
                            .put("subval111", 33)
                            .put("subval112", "string3")
                            .build())
                    .build())
            .put("node2.sub21.subval211", false)
            .put("node3", ImmutableMap.builder().put("sub31", 44).build())
            .build();
    Map<String, Object> updated = ConfigUtil.extend(map, update);
    assertEquals(1, ConfigUtil.value(updated, "node1.val1"));
    assertEquals("string", ConfigUtil.value(updated, "node1.val2"));
    assertEquals(false, ConfigUtil.value(updated, "node2.sub21.subval211"));
    assertEquals(1.2d, ConfigUtil.value(updated, "node2.sub21.subval212"));
    assertEquals("string2", ConfigUtil.value(updated, "node2.sub2"));
    assertEquals(44, ConfigUtil.value(updated, "node3.sub31"));
    assertEquals(33, ConfigUtil.value(updated, "node1.sub11.subval111"));
    assertEquals("string3", ConfigUtil.value(updated, "node1.sub11.subval112"));
  }
}
