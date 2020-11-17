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
package com.datastax.oss.sink.util;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import java.util.Map;
import java.util.TreeMap;

/** Utility methods for manipulating strings. */
public class StringUtil {
  /** This is a utility class and should never be instantiated. */
  private StringUtil() {}

  public static String singleQuote(String s) {
    return "'" + s + "'";
  }

  public static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }

  public static void printMap(Map<String, ?> map) {
    for (Map.Entry<String, ?> et : map.entrySet()) printNode(et, 0);
  }

  @SuppressWarnings("unchecked")
  private static void printNode(Map.Entry<String, ?> et, int dept) {
    boolean ismap = et.getValue() instanceof Map;
    String val;
    if (ismap) val = "";
    else if (et.getValue() == null) val = " = null";
    else val = " = " + et.getValue().getClass().getSimpleName() + "[" + et.getValue() + "]";
    System.out.println(Strings.repeat(" ", dept) + et.getKey() + val);
    if (ismap) {
      for (Map.Entry<String, ?> et1 : ((Map<String, ?>) et.getValue()).entrySet()) {
        printNode(et1, dept + 2);
      }
    }
  }

  public static Map<String, String> flatString(Map<String, ?> map) {
    Map<String, String> flat = new TreeMap<>();
    for (Map.Entry<String, ?> et : map.entrySet()) flatNode(et, null, flat);
    return flat;
  }

  @SuppressWarnings("unchecked")
  private static void flatNode(Map.Entry<String, ?> node, String key, Map<String, String> acc) {
    String nkey = key == null ? node.getKey() : String.join(".", key, node.getKey());
    if (node.getValue() == null) return; // acc.put(nkey, null);
    else if (node.getValue() instanceof Map) {
      for (Map.Entry<String, ?> et : ((Map<String, ?>) node.getValue()).entrySet()) {
        flatNode(et, nkey, acc);
      }
    } else {
      acc.put(nkey, String.valueOf(node.getValue()));
    }
  }
}
