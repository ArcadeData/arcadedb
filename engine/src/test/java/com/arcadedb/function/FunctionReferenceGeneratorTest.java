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
package com.arcadedb.function;

import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generates function-reference.json for Studio autocomplete and function reference panel.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FunctionReferenceGeneratorTest {

  @Test
  void generateFunctionReference() throws IOException {
    final JSONObject root = new JSONObject();
    root.put("generated", LocalDate.now().toString());

    final JSONObject categories = new JSONObject();
    final TreeSet<String> autocompleteNames = new TreeSet<>();

    // --- SQL Functions ---
    final JSONObject sqlFunctionCategories = new JSONObject();
    final DefaultSQLFunctionFactory sqlFactory = DefaultSQLFunctionFactory.getInstance();
    final Map<String, Object> sqlFunctions = sqlFactory.getFunctions();
    final TreeMap<String, JSONArray> sqlCatMap = new TreeMap<>();

    for (final Map.Entry<String, Object> entry : sqlFunctions.entrySet()) {
      final String name = entry.getKey();
      final Object value = entry.getValue();

      SQLFunction func;
      if (value instanceof SQLFunction f)
        func = f;
      else if (value instanceof Class<?> clazz) {
        try {
          func = (SQLFunction) clazz.getConstructor().newInstance();
        } catch (final Exception e) {
          continue;
        }
      } else
        continue;

      final String category = deriveSqlFunctionCategory(func);
      autocompleteNames.add(name);

      final JSONObject entry2 = new JSONObject();
      entry2.put("name", name);
      entry2.put("syntax", func.getSyntax());
      final String desc = func.getDescription();
      if (desc != null && !desc.isEmpty())
        entry2.put("description", desc);
      entry2.put("since", "sql");

      sqlCatMap.computeIfAbsent(category, k -> new JSONArray()).put(entry2);
    }

    for (final Map.Entry<String, JSONArray> e : sqlCatMap.entrySet())
      sqlFunctionCategories.put(e.getKey(), e.getValue());

    categories.put("SQL Functions", sqlFunctionCategories);

    // --- Cypher Functions ---
    final JSONObject cypherCategories = new JSONObject();
    final TreeMap<String, JSONArray> cypherCatMap = new TreeMap<>();

    for (final StatelessFunction func : CypherFunctionRegistry.getAllFunctions()) {
      final String name = func.getName();
      final String category = deriveCypherCategory(name);
      autocompleteNames.add(name);

      final JSONObject entry2 = new JSONObject();
      entry2.put("name", name);
      entry2.put("syntax", func.getSyntax());
      final String desc = func.getDescription();
      if (desc != null && !desc.isEmpty())
        entry2.put("description", desc);
      entry2.put("since", "cypher");

      cypherCatMap.computeIfAbsent(category, k -> new JSONArray()).put(entry2);
    }

    for (final Map.Entry<String, JSONArray> e : cypherCatMap.entrySet())
      cypherCategories.put(e.getKey(), e.getValue());

    categories.put("Cypher Functions", cypherCategories);

    // --- SQL Methods ---
    final JSONObject methodCategories = new JSONObject();
    final Map<String, Object> sqlMethods = DefaultSQLMethodFactory.getInstance().getMethods();
    final TreeMap<String, JSONArray> methodCatMap = new TreeMap<>();

    for (final Map.Entry<String, Object> entry : sqlMethods.entrySet()) {
      final String name = entry.getKey();
      final Object value = entry.getValue();

      SQLMethod method;
      if (value instanceof SQLMethod m)
        method = m;
      else if (value instanceof Class<?> clazz) {
        try {
          method = (SQLMethod) clazz.getConstructor().newInstance();
        } catch (final Exception e) {
          continue;
        }
      } else
        continue;

      final String category = deriveMethodCategory(method);
      autocompleteNames.add(name);

      final JSONObject entry2 = new JSONObject();
      entry2.put("name", name);
      entry2.put("syntax", method.getSyntax());
      entry2.put("since", "sql");

      methodCatMap.computeIfAbsent(category, k -> new JSONArray()).put(entry2);
    }

    for (final Map.Entry<String, JSONArray> e : methodCatMap.entrySet())
      methodCategories.put(e.getKey(), e.getValue());

    categories.put("SQL Methods", methodCategories);

    root.put("categories", categories);

    // --- Autocomplete list ---
    final JSONArray autocomplete = new JSONArray();
    for (final String name : autocompleteNames)
      autocomplete.put(name);
    root.put("autocomplete", autocomplete);

    // Write to Studio resources
    final String outputPath = System.getProperty("user.dir") + "/../studio/src/main/resources/static/js/function-reference.json";
    final File outputFile = new File(outputPath).getCanonicalFile();
    outputFile.getParentFile().mkdirs();

    try (final FileWriter writer = new FileWriter(outputFile)) {
      writer.write(root.toString(2));
    }

    assertThat(outputFile).exists();
    assertThat(outputFile.length()).isGreaterThan(100);
    assertThat(autocompleteNames).isNotEmpty();
    assertThat(autocompleteNames.size()).isGreaterThan(50);

    // Verify we got functions from all three sources
    assertThat(sqlCatMap).isNotEmpty();
    assertThat(cypherCatMap).isNotEmpty();
    assertThat(methodCatMap).isNotEmpty();
  }

  private static String deriveSqlFunctionCategory(final SQLFunction func) {
    // Check function name prefix for reflective functions
    final String funcName = func.getName();
    if (funcName.startsWith("math_"))
      return "Math";
    if (funcName.startsWith("vector.") || funcName.startsWith("vector"))
      return "Vector";

    final String className = func.getClass().getPackageName();
    final String lastPkg = className.substring(className.lastIndexOf('.') + 1);
    return switch (lastPkg) {
      case "math" -> "Math";
      case "coll" -> "Collection";
      case "graph" -> "Graph";
      case "geo" -> "Geo";
      case "text" -> "Text";
      case "time" -> "Time";
      case "misc" -> "Misc";
      case "vector" -> "Vector";
      default -> capitalize(lastPkg);
    };
  }

  private static String deriveCypherCategory(final String name) {
    final int dot = name.indexOf('.');
    if (dot > 0) {
      final String prefix = name.substring(0, dot);
      return capitalize(prefix);
    }
    return "General";
  }

  private static String deriveMethodCategory(final SQLMethod method) {
    final String className = method.getClass().getPackageName();
    final String lastPkg = className.substring(className.lastIndexOf('.') + 1);
    return switch (lastPkg) {
      case "string" -> "String";
      case "collection" -> "Collection";
      case "conversion" -> "Conversion";
      case "geo" -> "Geo";
      case "misc" -> "Misc";
      default -> capitalize(lastPkg);
    };
  }

  private static String capitalize(final String s) {
    if (s == null || s.isEmpty())
      return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }
}
