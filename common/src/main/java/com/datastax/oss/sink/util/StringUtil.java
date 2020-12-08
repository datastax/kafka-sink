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

import java.util.Scanner;
import java.util.regex.Matcher;

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

  public static String bytesToString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) sb.append(String.format("\\u%04x", b));
    return sb.toString();
  }

  public static byte[] stringToBytes(String string) {
    if (!string.startsWith("\\u"))
      throw new IllegalArgumentException("doesn't look like a byte array: " + string);
    byte[] bytes = new byte[string.length() / 5];
    try (Scanner scanner = new Scanner(string).useDelimiter(Matcher.quoteReplacement("\\u"))) {
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = scanner.nextByte(16);
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException("could not convert");
    }
    return bytes;
  }
}
