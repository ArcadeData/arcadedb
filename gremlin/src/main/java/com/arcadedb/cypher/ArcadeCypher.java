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
package com.arcadedb.cypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.cypher.query.CypherQueryEngine;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeGremlin;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.TranslationFacade;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.translator.Translator;
import org.opencypher.v9_0.util.SyntaxException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Cypher Expression builder. Transform a cypher expression into Gremlin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeCypher extends ArcadeGremlin {
  private static final Map<String, CachedStatement> STATEMENT_CACHE = new ConcurrentHashMap<>();
  private static final int                          CACHE_SIZE      = GlobalConfiguration.CYPHER_STATEMENT_CACHE.getValueAsInteger();
  private              String                       cypher;

  private static class CachedStatement {
    public final String cypher;
    public final String gremlin;
    public       int    used = 0;

    private CachedStatement(final String cypher, final String gremlin) {
      this.cypher = cypher;
      this.gremlin = gremlin;
    }
  }

  public ArcadeCypher(final ArcadeGraph graph, final String cypherQuery) {
    super(graph, null);
    this.cypher = cypherQuery;
  }

  @Override
  public ResultSet execute() {
    if (query == null) {
      try {
        compileToGremlin(graph, parameters);
      } catch (final SyntaxException e) {
        throw new CommandParsingException(e);
      }

    }
    final ResultSet resultSet = super.execute();
    final InternalResultSet result = new InternalResultSet() {
      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return resultSet.getExecutionPlan();
      }
    };

    while (resultSet.hasNext()) {
      final Result next = resultSet.next();
      if (next.isElement())
        result.add(next);
      else {
        // unpack single result projections
        final Map<String, Object> map = next.toMap();
        final Object nextValue = map.values().iterator().next();
        if (map.size() == 1 && nextValue instanceof Map<?, ?> map1) {
          result.addAll(CypherQueryEngine.transformMap(map1));
        } else {
          result.addAll(CypherQueryEngine.transformMap(map));
        }
      }
    }
    return result;
  }

  @Override
  public QueryEngine.AnalyzedQuery parse() {
    if (query == null) {
      try {
        compileToGremlin(graph, parameters);
      } catch (final SyntaxException e) {
        throw new CommandParsingException(e);
      }
    }

    return super.parse();
  }

  record CypherQueryAndParameters(String cypherQuery, Map<String, Object> parameters) {
  }

  public void compileToGremlin(final ArcadeGraph graph, final Map<String, Object> parameters) {

    CypherQueryAndParameters queryAndParameters = replaceParameterNames(cypher, Optional.ofNullable(parameters).orElse(Map.of()));

    // Expand ALL(k IN keys($param) WHERE ...) patterns before translation
    queryAndParameters = expandAllKeysPattern(queryAndParameters);

    this.parameters = queryAndParameters.parameters;
    if (CACHE_SIZE == 0)
      // NO CACHE
      query = parameters != null ?
          new TranslationFacade().toGremlinGroovy(queryAndParameters.cypherQuery, queryAndParameters.parameters) :
          new TranslationFacade().toGremlinGroovy(queryAndParameters.cypherQuery);

    final String db = graph.getDatabase().getDatabasePath();

    final String mapKey = db + ":" + queryAndParameters.cypherQuery;

    final CachedStatement cached = STATEMENT_CACHE.get(mapKey);
    // FOUND
    if (cached != null) {
      ++cached.used;
      query = cached.gremlin;
    }

    // TRANSLATE TO GREMLIN AND CACHE THE STATEMENT FOR FURTHER USAGE
    final CypherAst ast = parameters == null ?
        CypherAst.parse(queryAndParameters.cypherQuery) :
        CypherAst.parse(queryAndParameters.cypherQuery, queryAndParameters.parameters);
    final Translator<String, GroovyPredicate> translator = Translator.builder().gremlinGroovy().enableCypherExtensions().build();
    String gremlin = ast.buildTranslation(translator);

    // REPLACE '  cypher.null' WITH NULL
    gremlin = gremlin.replace("'  cypher.null'", "null");

    cacheLastStatement(cypher, gremlin);

    query = gremlin;
  }

  /**
   * Expands ALL(k IN keys($param) WHERE var[k] = $param[k]) patterns into explicit property comparisons.
   * This works around opencypher-gremlin's lack of support for this pattern.
   * <p>
   * Example: ALL(k IN keys($props) WHERE n[k] = $props[k]) with $props={age:30, city:"NYC"}
   * becomes: n.age = $props_age AND n.city = $props_city
   */
  private static CypherQueryAndParameters expandAllKeysPattern(CypherQueryAndParameters queryAndParams) {
    String query = queryAndParams.cypherQuery;
    Map<String, Object> parameters = new HashMap<>(queryAndParams.parameters);

    // Pattern: ALL(variableName IN keys($paramName) WHERE nodeVar[variableName] = $paramName[variableName])
    // We need to match this pattern and expand it
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
        "ALL\\s*\\(\\s*(\\w+)\\s+IN\\s+keys\\s*\\(\\s*\\$([\\w]+)\\s*\\)\\s+WHERE\\s+(\\w+)\\s*\\[\\s*\\1\\s*\\]\\s*=\\s*\\$\\2\\s*\\[\\s*\\1\\s*\\]\\s*\\)",
        java.util.regex.Pattern.CASE_INSENSITIVE
    );

    java.util.regex.Matcher matcher = pattern.matcher(query);
    StringBuilder result = new StringBuilder();
    int lastEnd = 0;

    while (matcher.find()) {
      String iterVar = matcher.group(1);        // e.g., "k"
      String paramName = matcher.group(2);      // e.g., "props" (the randomized parameter name)
      String nodeVar = matcher.group(3);        // e.g., "n"

      // Get the parameter value (should be a Map)
      Object paramValue = parameters.get(paramName);

      if (paramValue instanceof Map<?, ?> paramMap) {
        // Build the expanded condition
        StringBuilder expanded = new StringBuilder();
        boolean first = true;

        for (Map.Entry<?, ?> entry : paramMap.entrySet()) {
          String key = entry.getKey().toString();
          Object value = entry.getValue();

          if (!first) {
            expanded.append(" AND ");
          }
          first = false;

          // Create a new parameter for this specific key
          String newParamName = paramName + "_" + key;
          expanded.append(nodeVar).append(".").append(key).append(" = $").append(newParamName);
          parameters.put(newParamName, value);
        }

        // If the map is empty, this should always be true (vacuous truth)
        if (paramMap.isEmpty()) {
          expanded.append("true");
        }

        // Replace the ALL pattern with the expanded condition
        result.append(query, lastEnd, matcher.start());
        result.append("(").append(expanded).append(")");
        lastEnd = matcher.end();
      } else {
        // If parameter is not a map, leave the pattern as is (will likely fail during translation)
        result.append(query, lastEnd, matcher.end());
        lastEnd = matcher.end();
      }
    }

    result.append(query.substring(lastEnd));
    return new CypherQueryAndParameters(result.toString(), parameters);
  }

  public static CypherQueryAndParameters replaceParameterNames(String cypher, Map<String, Object> parameters) {
    Map<String, String> replacementMap = new HashMap<>();
    String newCypher = cypher;

    for (String oldKey : parameters.keySet()) {
      // Generate a random 4-character string
      String newKey;
      do {
        newKey = generateRandomString(4);
      } while (replacementMap.containsValue(newKey)); // Ensure uniqueness

      // Replace occurrences of the old key in the cypher cypherQuery
      String placeholder = "\\$" + oldKey; // Escape the $ symbol for regex
      newCypher = newCypher.replaceAll(placeholder, "\\$" + newKey);

      // Add the new key to the replacement map
      replacementMap.put(oldKey, newKey);
    }

    // Update the parameters map with the new keys
    Map<String, Object> updatedParameters = new HashMap<>();
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      updatedParameters.put(replacementMap.get(entry.getKey()), entry.getValue());
    }

    return new CypherQueryAndParameters(newCypher, updatedParameters);
  }

  private static String generateRandomString(int length) {
    String chars = "abcdefghijklmnopqrstuvwxyz";
    StringBuilder sb = new StringBuilder(length);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  public static void closeDatabase(final ArcadeGraph graph) {
    final String mapKey = graph.getDatabase().getDatabasePath() + ":";

    // REMOVE ALL THE ENTRIES RELATIVE TO THE CLOSED DATABASE
    STATEMENT_CACHE.entrySet().removeIf(stringCachedStatementEntry -> stringCachedStatementEntry.getKey().startsWith(mapKey));
  }

  private static void cacheLastStatement(String cypher, String gremlin) {
    if (CACHE_SIZE > 0) {
      // REMOVE THE OLDEST TO MAKE ROOM FOR THE LATEST STATEMENT
      while (STATEMENT_CACHE.size() >= CACHE_SIZE) {
        int leastUsedValue = 0;
        String leastUsedKey = null;

        for (final Map.Entry<String, CachedStatement> entry : STATEMENT_CACHE.entrySet()) {
          if (leastUsedKey == null || entry.getValue().used < leastUsedValue) {
            leastUsedKey = entry.getKey();
            leastUsedValue = entry.getValue().used;
          }
        }
        STATEMENT_CACHE.remove(leastUsedKey);
      }
      STATEMENT_CACHE.put(cypher, new CachedStatement(cypher, gremlin));
    }
  }
}
