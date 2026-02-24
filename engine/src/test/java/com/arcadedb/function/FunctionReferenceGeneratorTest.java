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

import com.arcadedb.query.sql.parser.SqlParserConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generates function-reference.json for Studio autocomplete and function reference panel.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FunctionReferenceGeneratorTest {

  @Test
  void generateFunctionReference() throws Exception {
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

    // --- SQL Keywords from SqlParserConstants.tokenImage ---
    final TreeSet<String> sqlKeywords = extractSqlKeywords();
    final JSONArray sqlKeywordsArray = new JSONArray();
    for (final String kw : sqlKeywords)
      sqlKeywordsArray.put(kw);
    root.put("sqlKeywords", sqlKeywordsArray);

    // --- Cypher Keywords from Cypher25Lexer.g4 ---
    final TreeSet<String> cypherKeywords = extractCypherKeywords();
    final JSONArray cypherKeywordsArray = new JSONArray();
    for (final String kw : cypherKeywords)
      cypherKeywordsArray.put(kw);
    root.put("cypherKeywords", cypherKeywordsArray);

    // --- SQL Grammar Tree ---
    root.put("sqlGrammar", buildSqlGrammarTree());

    // --- Cypher Grammar Tree ---
    root.put("cypherGrammar", buildCypherGrammarTree());

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

    // Verify grammar data
    assertThat(sqlKeywords).isNotEmpty();
    assertThat(sqlKeywords.size()).isGreaterThan(30);
    assertThat(cypherKeywords).isNotEmpty();
    assertThat(cypherKeywords.size()).isGreaterThan(30);
    assertThat(root.has("sqlGrammar")).isTrue();
    assertThat(root.has("cypherGrammar")).isTrue();
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

  private static TreeSet<String> extractSqlKeywords() {
    final TreeSet<String> keywords = new TreeSet<>();
    final Set<String> wellKnownTokenKeywords = Set.of(
        "not", "in", "like", "ilike", "is", "between", "contains", "containsall",
        "containsany", "containskey", "containsvalue", "containstext", "matches",
        "key", "item", "instanceof", "true", "false", "order by", "group by");

    for (final String image : SqlParserConstants.tokenImage) {
      if (image.startsWith("\"") && image.endsWith("\"") && image.length() > 2) {
        final String raw = image.substring(1, image.length() - 1);
        if (raw.matches("[a-zA-Z_]+"))
          keywords.add(raw.toLowerCase());
      }
    }
    keywords.addAll(wellKnownTokenKeywords);
    return keywords;
  }

  private static TreeSet<String> extractCypherKeywords() throws IOException {
    final TreeSet<String> keywords = new TreeSet<>();
    final String lexerPath = System.getProperty("user.dir")
        + "/src/main/antlr4/com/arcadedb/query/opencypher/grammar/Cypher25Lexer.g4";
    final File lexerFile = new File(lexerPath);
    if (!lexerFile.exists())
      return keywords;

    final Pattern tokenPattern = Pattern.compile("^([A-Z][A-Z0-9_]*)$");
    final Set<String> skipTokens = Set.of("SPACE", "IDENTIFIER", "EXTENDED_IDENTIFIER", "ErrorChar", "EOF");
    try (final BufferedReader reader = new BufferedReader(new FileReader(lexerFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final String trimmed = line.trim();
        // Skip fragment rules (single-letter fragments, etc.)
        if (trimmed.startsWith("fragment "))
          continue;
        final Matcher m = tokenPattern.matcher(trimmed);
        if (m.matches()) {
          final String tokenName = m.group(1);
          // Skip non-keyword tokens
          if (tokenName.contains("LITERAL") || tokenName.contains("COMMENT")
              || tokenName.contains("INTEGER") || tokenName.contains("DECIMAL")
              || skipTokens.contains(tokenName) || tokenName.startsWith("ESCAPED_")
              || tokenName.startsWith("UNSIGNED_") || tokenName.startsWith("ARROW_"))
            continue;
          keywords.add(tokenName);
        }
      }
    }
    return keywords;
  }

  private static JSONObject buildSqlGrammarTree() {
    final JSONObject tree = new JSONObject();

    tree.put("_start", grammarNode(new String[] { "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP",
        "TRUNCATE", "MATCH", "TRAVERSE", "BEGIN", "COMMIT", "ROLLBACK",
        "LET", "RETURN", "IF", "EXPLAIN", "PROFILE", "REBUILD",
        "IMPORT", "EXPORT", "BACKUP", "CHECK", "ALIGN", "DEFINE",
        "MOVE", "LOCK", "SLEEP", "REFRESH" }, null));

    tree.put("SELECT", grammarNode(new String[] { "DISTINCT", "*", "FROM" }, new String[] { "expression", "function" }));
    tree.put("SELECT._projection", grammarNode(new String[] { "FROM", "AS", "," }, new String[] { "expression", "function" }));
    tree.put("SELECT.FROM", grammarNode(new String[] { "BUCKET" }, new String[] { "type", "subquery" }));
    tree.put("SELECT.FROM._target", grammarNode(new String[] { "WHERE", "LET", "GROUP BY", "ORDER BY", "UNWIND", "SKIP", "LIMIT", "TIMEOUT", "AS" }, new String[] { "expression" }));
    tree.put("SELECT.WHERE", grammarNode(new String[] { "AND", "OR", "NOT", "IS", "IN", "BETWEEN", "LIKE", "ILIKE",
        "CONTAINS", "CONTAINSALL", "CONTAINSANY", "CONTAINSKEY",
        "CONTAINSVALUE", "CONTAINSTEXT", "MATCHES", "INSTANCEOF",
        "NULL", "TRUE", "FALSE", "DEFINED" }, new String[] { "expression", "property", "function" }));
    tree.put("SELECT.WHERE._after", grammarNode(new String[] { "AND", "OR", "GROUP BY", "ORDER BY", "UNWIND", "SKIP", "LIMIT", "TIMEOUT" }, null));
    tree.put("SELECT.GROUP_BY", grammarNode(new String[] { "ORDER BY", "UNWIND", "SKIP", "LIMIT", "TIMEOUT" }, new String[] { "expression" }));
    tree.put("SELECT.ORDER_BY", grammarNode(new String[] { "ASC", "DESC", "UNWIND", "SKIP", "LIMIT", "TIMEOUT", "," }, new String[] { "expression" }));
    tree.put("SELECT.UNWIND", grammarNode(new String[] { "AS", "SKIP", "LIMIT", "TIMEOUT" }, new String[] { "expression" }));
    tree.put("SELECT.LIMIT", grammarNode(new String[] { "SKIP", "TIMEOUT" }, null));
    tree.put("SELECT.SKIP", grammarNode(new String[] { "LIMIT", "TIMEOUT" }, null));

    tree.put("INSERT", grammarNode(new String[] { "INTO" }, null));
    tree.put("INSERT.INTO", grammarNode(new String[] { "BUCKET" }, new String[] { "type" }));
    tree.put("INSERT.INTO._target", grammarNode(new String[] { "SET", "VALUES", "CONTENT", "RETURN", "FROM", "UNSAFE", "(" }, null));
    tree.put("INSERT.SET", grammarNode(new String[] { ",", "RETURN", "FROM", "UNSAFE" }, new String[] { "property" }));
    tree.put("INSERT.CONTENT", grammarNode(new String[] { "RETURN", "FROM", "UNSAFE" }, null));
    tree.put("INSERT.VALUES", grammarNode(new String[] { ",", "RETURN", "FROM", "UNSAFE" }, null));

    tree.put("UPDATE", grammarNode(null, new String[] { "type" }));
    tree.put("UPDATE._target", grammarNode(new String[] { "SET", "ADD", "PUT", "REMOVE", "INCREMENT", "MERGE", "CONTENT", "UPSERT", "RETURN", "WHERE", "LIMIT", "TIMEOUT" }, null));
    tree.put("UPDATE.SET", grammarNode(new String[] { ",", "ADD", "PUT", "REMOVE", "INCREMENT", "MERGE", "CONTENT", "UPSERT", "RETURN", "WHERE", "LIMIT", "TIMEOUT" }, new String[] { "property" }));
    tree.put("UPDATE.WHERE", grammarNode(new String[] { "AND", "OR", "NOT", "LIMIT", "TIMEOUT" }, null));

    tree.put("DELETE", grammarNode(new String[] { "VERTEX", "FROM" }, null));
    tree.put("DELETE.FROM", grammarNode(new String[] { "RETURN", "WHERE", "LIMIT", "UNSAFE" }, new String[] { "type" }));
    tree.put("DELETE.WHERE", grammarNode(new String[] { "AND", "OR", "NOT", "LIMIT", "UNSAFE" }, null));

    tree.put("CREATE", grammarNode(new String[] { "VERTEX", "EDGE", "DOCUMENT", "PROPERTY", "INDEX", "BUCKET", "TRIGGER", "MATERIALIZED" }, null));
    tree.put("CREATE.DOCUMENT", grammarNode(new String[] { "TYPE" }, null));
    tree.put("CREATE.VERTEX", grammarNode(new String[] { "TYPE" }, null));
    tree.put("CREATE.EDGE", grammarNode(new String[] { "TYPE" }, null));
    tree.put("CREATE.VERTEX_TYPE", grammarNode(new String[] { "IF" }, new String[] { "identifier" }));
    tree.put("CREATE.VERTEX_TYPE._name", grammarNode(new String[] { "IF", "EXTENDS", "BUCKET", "BUCKETS", "PAGESIZE" }, null));
    tree.put("CREATE.EDGE_TYPE", grammarNode(new String[] { "IF" }, new String[] { "identifier" }));
    tree.put("CREATE.EDGE_TYPE._name", grammarNode(new String[] { "IF", "EXTENDS", "UNIDIRECTIONAL", "BUCKET", "BUCKETS", "PAGESIZE" }, null));
    tree.put("CREATE.DOCUMENT_TYPE", grammarNode(new String[] { "IF" }, new String[] { "identifier" }));
    tree.put("CREATE.DOCUMENT_TYPE._name", grammarNode(new String[] { "IF", "EXTENDS", "BUCKET", "BUCKETS", "PAGESIZE" }, null));
    tree.put("CREATE.PROPERTY", grammarNode(null, new String[] { "type" }));
    tree.put("CREATE.INDEX", grammarNode(new String[] { "ON", "IF" }, null));
    tree.put("CREATE.INDEX.ON", grammarNode(new String[] { "TYPE" }, new String[] { "type" }));
    tree.put("CREATE.MATERIALIZED", grammarNode(new String[] { "VIEW" }, null));
    tree.put("CREATE.MATERIALIZED_VIEW", grammarNode(new String[] { "IF" }, new String[] { "identifier" }));
    tree.put("CREATE.MATERIALIZED_VIEW._name", grammarNode(new String[] { "AS" }, null));

    tree.put("ALTER", grammarNode(new String[] { "TYPE", "PROPERTY", "BUCKET", "DATABASE", "MATERIALIZED" }, null));
    tree.put("ALTER.TYPE", grammarNode(null, new String[] { "type" }));
    tree.put("ALTER.TYPE._name", grammarNode(new String[] { "NAME", "SUPERTYPE", "BUCKETSELECTIONSTRATEGY", "BUCKET", "CUSTOM", "ALIASES" }, null));
    tree.put("ALTER.PROPERTY", grammarNode(null, new String[] { "type" }));
    tree.put("ALTER.BUCKET", grammarNode(null, new String[] { "identifier" }));
    tree.put("ALTER.DATABASE", grammarNode(null, new String[] { "identifier" }));
    tree.put("ALTER.MATERIALIZED", grammarNode(new String[] { "VIEW" }, null));

    tree.put("DROP", grammarNode(new String[] { "TYPE", "PROPERTY", "INDEX", "BUCKET", "TRIGGER", "MATERIALIZED" }, null));
    tree.put("DROP.TYPE", grammarNode(new String[] { "IF" }, new String[] { "type" }));
    tree.put("DROP.PROPERTY", grammarNode(new String[] { "IF" }, new String[] { "type" }));
    tree.put("DROP.INDEX", grammarNode(new String[] { "IF" }, new String[] { "identifier" }));
    tree.put("DROP.MATERIALIZED", grammarNode(new String[] { "VIEW" }, null));

    tree.put("TRUNCATE", grammarNode(new String[] { "TYPE", "BUCKET", "RECORD" }, null));
    tree.put("TRUNCATE.TYPE", grammarNode(new String[] { "POLYMORPHIC", "UNSAFE" }, new String[] { "type" }));
    tree.put("TRUNCATE.BUCKET", grammarNode(new String[] { "UNSAFE" }, new String[] { "identifier" }));

    tree.put("MATCH", grammarNode(new String[] { "{" }, new String[] { "function" }));
    tree.put("MATCH._pattern", grammarNode(new String[] { "RETURN", ",", "-->", "<--", "--", "-" }, null));
    tree.put("MATCH.RETURN", grammarNode(new String[] { "DISTINCT", "GROUP BY", "ORDER BY", "SKIP", "LIMIT", "AS", "," }, new String[] { "expression" }));

    tree.put("TRAVERSE", grammarNode(new String[] { "FROM" }, new String[] { "expression", "function" }));
    tree.put("TRAVERSE.FROM", grammarNode(new String[] { "MAXDEPTH", "WHILE", "LIMIT", "STRATEGY" }, new String[] { "type" }));
    tree.put("TRAVERSE.WHILE", grammarNode(new String[] { "LIMIT", "STRATEGY" }, new String[] { "expression" }));
    tree.put("TRAVERSE.STRATEGY", grammarNode(new String[] { "DEPTH_FIRST", "BREADTH_FIRST" }, null));

    tree.put("BEGIN", grammarNode(new String[] { "ISOLATION" }, null));
    tree.put("COMMIT", grammarNode(new String[] { "RETRY" }, null));
    tree.put("REBUILD", grammarNode(new String[] { "INDEX" }, null));
    tree.put("EXPLAIN", grammarNode(new String[] { "SELECT", "INSERT", "UPDATE", "DELETE", "MATCH", "TRAVERSE" }, null));
    tree.put("PROFILE", grammarNode(new String[] { "SELECT", "INSERT", "UPDATE", "DELETE", "MATCH", "TRAVERSE" }, null));
    tree.put("IMPORT", grammarNode(new String[] { "DATABASE" }, null));
    tree.put("EXPORT", grammarNode(new String[] { "DATABASE" }, null));
    tree.put("BACKUP", grammarNode(new String[] { "DATABASE" }, null));
    tree.put("CHECK", grammarNode(new String[] { "DATABASE" }, null));
    tree.put("CHECK.DATABASE", grammarNode(new String[] { "TYPE", "BUCKET", "FIX", "COMPRESS" }, null));
    tree.put("LOCK", grammarNode(new String[] { "TYPE", "BUCKET" }, null));
    tree.put("DEFINE", grammarNode(new String[] { "FUNCTION" }, null));
    tree.put("REFRESH", grammarNode(new String[] { "MATERIALIZED" }, null));
    tree.put("REFRESH.MATERIALIZED", grammarNode(new String[] { "VIEW" }, null));

    return tree;
  }

  private static JSONObject buildCypherGrammarTree() {
    final JSONObject tree = new JSONObject();

    tree.put("_start", grammarNode(new String[] { "MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "REMOVE", "SET",
        "WITH", "UNWIND", "RETURN", "OPTIONAL", "CALL", "UNION", "FOREACH" }, null));
    tree.put("MATCH", grammarNode(new String[] { "(", "WHERE", "RETURN", "WITH", "CREATE", "DELETE", "SET", "REMOVE", "MERGE", "OPTIONAL", "UNWIND" }, null));
    tree.put("MATCH._pattern", grammarNode(new String[] { "WHERE", "RETURN", "WITH", ",", "CREATE", "DELETE", "SET", "REMOVE" }, null));
    tree.put("WHERE", grammarNode(new String[] { "AND", "OR", "NOT", "XOR", "IS", "IN", "CONTAINS", "STARTS", "ENDS",
        "RETURN", "WITH", "CREATE", "DELETE", "SET", "REMOVE" }, null));
    tree.put("RETURN", grammarNode(new String[] { "DISTINCT", "AS", "ORDER BY", "SKIP", "LIMIT", ",", "UNION" }, new String[] { "expression" }));
    tree.put("WITH", grammarNode(new String[] { "DISTINCT", "AS", "WHERE", "ORDER BY", "SKIP", "LIMIT", ",",
        "MATCH", "CREATE", "DELETE", "SET", "REMOVE", "MERGE", "UNWIND", "RETURN" }, null));
    tree.put("CREATE", grammarNode(new String[] { "(", "RETURN", "WITH", "SET", "DELETE", "REMOVE", "MERGE" }, null));
    tree.put("SET", grammarNode(new String[] { ",", "RETURN", "WITH", "DELETE", "REMOVE", "CREATE" }, new String[] { "property" }));
    tree.put("DELETE", grammarNode(new String[] { "RETURN", "WITH", "CREATE", "SET", "REMOVE" }, null));
    tree.put("UNWIND", grammarNode(new String[] { "AS" }, new String[] { "expression" }));
    tree.put("UNWIND.AS", grammarNode(new String[] { "MATCH", "CREATE", "DELETE", "SET", "REMOVE", "MERGE", "RETURN", "WITH", "UNWIND" }, null));
    tree.put("ORDER_BY", grammarNode(new String[] { "ASC", "DESC", "ASCENDING", "DESCENDING", "SKIP", "LIMIT", "," }, null));

    return tree;
  }

  private static JSONObject grammarNode(final String[] keywords, final String[] expect) {
    final JSONObject node = new JSONObject();
    if (keywords != null && keywords.length > 0) {
      final JSONArray kw = new JSONArray();
      for (final String k : keywords)
        kw.put(k);
      node.put("keywords", kw);
    }
    if (expect != null && expect.length > 0) {
      final JSONArray exp = new JSONArray();
      for (final String e : expect)
        exp.put(e);
      node.put("expect", exp);
    }
    return node;
  }

  private static String capitalize(final String s) {
    if (s == null || s.isEmpty())
      return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }
}
