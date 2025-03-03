/*
 * Copyright (c) 2018-2025 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.gremlin.translation.groovy;

import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        if (argument instanceof List) {
            return ((List<?>) argument).stream()
                .map(StringTranslationUtils::toLiteral)
                .collect(Collectors.joining(", ", "[", "]"));
        }
        if (argument instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) argument;
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
        if (argument instanceof String) {
            return toStringLiteral((String) argument);
        }
        if (argument instanceof Double || argument instanceof Float) {
            return argument.toString() + "d";
        }
        if (argument == null) {
            return "null";
        }
        if (argument instanceof Verbatim) {
            return ((Verbatim) argument).getValue();
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
