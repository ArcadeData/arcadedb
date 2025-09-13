/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opencypher.gremlin.translation.groovy;

import java.util.*;
import java.util.stream.*;

import static java.util.stream.Collectors.*;

/**
 * This class overrides the class found in the OpenCypher-Gremlin project. OpenCypher-Gremlin is not active
 * and not willing to merge this security fix.
 *
 * @author @ExtReMLapin
 */
public final class StringTranslationUtils {
  private StringTranslationUtils() {
  }

  public static String apply(String name, Object... arguments) {
    String joined = Stream.of(arguments)
        .map(StringTranslationUtils::toLiteral)
        .collect(joining(", "));
    return name + "(" + joined + ")";
  }

  static String chain(String name, Object... arguments) {
    return "." + apply(name, arguments);
  }

  public static String toLiteral(Object argument) {
    if (argument instanceof List<?> list) {
      return list.stream()
          .map(StringTranslationUtils::toLiteral)
          .collect(Collectors.joining(", ", "[", "]"));
    }
    if (argument instanceof Map<?, ?> map) {
      if (map.isEmpty()) {
        return "[:]";
      }
      return map.entrySet().stream()
          .map(entry -> {
            Object key = entry.getKey();
            Object value = toLiteral(entry.getValue());
            return key + ": " + value;
          })
          .collect(Collectors.joining(", ", "[", "]"));
    }
    if (argument instanceof Double || argument instanceof Float) {
      return argument.toString() + "d";
    }
    if (argument instanceof String string) {
      return toStringLiteral(string);
    }
    if (argument instanceof Double || argument instanceof Float) {
      return argument.toString() + "d";
    }
    if (argument == null) {
      return "null";
    }
    if (argument instanceof Verbatim verbatim) {
      return verbatim.getValue();
    }

    return argument.toString();
  }

  private static String toStringLiteral(String argument) {
    if (argument.contains("\n")) {
      // Handle multiline strings
      if (argument.contains("\"\"\"")) {
        // If the string contains """, use single quotes and escape newlines and quotes
        return "'" + argument.replaceAll("(['\\\\])", "\\\\$1")
            .replaceAll("\n", "\\\\n")
            .replaceAll("\r", "\\\\r") + "'";
      } else {
        // Use triple quotes for multiline strings, but escape any ending triple quotes
        String processed = argument.replaceAll("\"\"\"", "\\\\\"\\\\\"\\\\\"");
        return "\"\"\"" + processed + "\"\"\"";
      }
    } else {
      // For single line strings, use the original approach with single quotes
      return "'" + argument.replaceAll("(['\\\\])", "\\\\$1") + "'";
    }
  }

}
