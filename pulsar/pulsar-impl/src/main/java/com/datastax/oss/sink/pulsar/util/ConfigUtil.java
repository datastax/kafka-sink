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

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.sink.util.Tuple2;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class ConfigUtil {

  public static Map<String, Object> extend(
      Map<String, Object> initial, Map<String, Object> update) {
    Map<String, Object> flatInitial = flat(initial);
    flatInitial.putAll(flat(update));
    return tree(flatInitial);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> copy(Map<String, Object> source) {
    return source
        .entrySet()
        .stream()
        .map(
            et -> {
              Object val = et.getValue();
              if (et.getValue() instanceof Map) val = copy((Map<String, Object>) et.getValue());
              return Tuple2.of(et.getKey(), val);
            })
        .collect(HashMap::new, (m, t) -> m.put(t._1, t._2), HashMap::putAll);
  }

  @SuppressWarnings("unchecked")
  public static void rename(Map<String, Object> config, String from, String to) {
    copy(config, from, to, true);
  }

  @SuppressWarnings("unchecked")
  private static void copy(Map<String, Object> config, String from, String to, boolean removeFrom) {
    Map<String, Object> parent = config;
    String from0 = from;
    int lastIdx = from.lastIndexOf('.');
    if (lastIdx != -1) {
      parent = (Map<String, Object>) value(config, from.substring(0, lastIdx));
      from0 = from.substring(lastIdx + 1);
    }
    Object node = parent.get(from0);
    if (node == null) throw new IllegalArgumentException("cannot find node on path [" + from + "]");
    if (removeFrom) parent.remove(from0);
    parent.put(to, node instanceof Map ? copy((Map<String, Object>) node) : node);
  }

  public static void copy(Map<String, Object> config, String from, String to) {
    copy(config, from, to, false);
  }

  public static void printMap(Map<String, ?> map) {
    printMap(map, System.out);
  }

  public static void printMap(Map<String, ?> map, PrintStream out) {
    for (Map.Entry<String, ?> et : map.entrySet()) printNode(et, 0, out);
  }

  @SuppressWarnings("unchecked")
  private static void printNode(Map.Entry<String, ?> et, int indent, PrintStream out) {
    boolean ismap = et.getValue() instanceof Map;
    String val;
    if (ismap) val = "";
    else if (et.getValue() == null) val = " = null";
    else val = " = " + et.getValue().getClass().getSimpleName() + "[" + et.getValue() + "]";
    out.println(Strings.repeat(" ", indent) + et.getKey() + val);
    if (ismap) {
      for (Map.Entry<String, ?> et1 : ((Map<String, ?>) et.getValue()).entrySet()) {
        printNode(et1, indent + 2, out);
      }
    }
  }

  public static Map<String, String> flatString(Map<String, ?> map) {
    Map<String, String> flat = new TreeMap<>();
    for (Map.Entry<String, ?> et : map.entrySet()) flatNodeString(et, null, flat);
    return flat;
  }

  @SuppressWarnings("unchecked")
  private static void flatNodeString(
      Map.Entry<String, ?> node, String key, Map<String, String> acc) {
    String nkey = key == null ? node.getKey() : String.join(".", key, node.getKey());
    if (node.getValue() == null) return; // acc.put(nkey, null);
    else if (node.getValue() instanceof Map) {
      for (Map.Entry<String, ?> et : ((Map<String, ?>) node.getValue()).entrySet()) {
        flatNodeString(et, nkey, acc);
      }
    } else {
      String sv =
          node.getValue() instanceof Double
                  && ((Double) node.getValue()).intValue() == (Double) node.getValue()
              ? String.valueOf(((Double) node.getValue()).intValue())
              : String.valueOf(node.getValue());
      acc.put(nkey, sv);
    }
  }

  public static Map<String, Object> flat(Map<String, ?> map) {
    Map<String, Object> flat = new TreeMap<>();
    for (Map.Entry<String, ?> et : map.entrySet()) flatNode(et, null, flat);
    return flat;
  }

  @SuppressWarnings("unchecked")
  private static void flatNode(Map.Entry<String, ?> node, String key, Map<String, Object> acc) {
    String nkey = key == null ? node.getKey() : String.join(".", key, node.getKey());
    if (node.getValue() == null) return; // acc.put(nkey, null);
    else if (node.getValue() instanceof Map) {
      for (Map.Entry<String, ?> et : ((Map<String, ?>) node.getValue()).entrySet()) {
        flatNode(et, nkey, acc);
      }
    } else {
      acc.put(nkey, node.getValue());
    }
  }

  public static Map<String, Object> tree(Map<String, Object> flat) {
    Map<String, Object> acc = new HashMap<>();
    for (Map.Entry<String, Object> et : flat.entrySet()) {
      LinkedList<String> path = new LinkedList<>(Arrays.asList(et.getKey().split("\\.")));
      buildNode(acc, path, et.getValue());
    }
    return acc;
  }

  private static void buildNode(Map<String, Object> acc, LinkedList<String> path, Object value) {
    if (path.size() == 1) acc.put(path.poll(), value);
    else {
      @SuppressWarnings("unchecked")
      Map<String, Object> node =
          (Map<String, Object>) acc.computeIfAbsent(path.poll(), k -> new HashMap<>());
      buildNode(node, path, value);
    }
  }

  @SuppressWarnings("unchecked")
  public static Object value(Map<String, Object> map, String path) {
    LinkedList<String> p = new LinkedList<>(Arrays.asList(path.split("\\.")));
    Map<String, Object> next = map;
    while (!p.isEmpty()) {
      Object o = next.get(p.poll());
      if (o instanceof Map) next = (Map<String, Object>) o;
      else return o;
    }
    return next;
  }
}
