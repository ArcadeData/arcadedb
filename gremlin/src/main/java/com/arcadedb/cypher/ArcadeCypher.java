/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.cypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeGremlin;
import com.arcadedb.cypher.query.CypherQueryEngine;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.TranslationFacade;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.translator.Translator;

import java.util.*;
import java.util.concurrent.*;

/**
 * Cypher Expression builder. Transform a cypher expression into Gremlin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeCypher extends ArcadeGremlin {
  private static final Map<String, CachedStatement> STATEMENT_CACHE = new ConcurrentHashMap<>();
  private static final int                          CACHE_SIZE      = GlobalConfiguration.CYPHER_STATEMENT_CACHE.getValueAsInteger();

  private static class CachedStatement {
    public final String cypher;
    public final String gremlin;
    public       int    used = 0;

    private CachedStatement(final String cypher, final String gremlin) {
      this.cypher = cypher;
      this.gremlin = gremlin;
    }
  }

  public ArcadeCypher(final ArcadeGraph graph, final String cypherQuery, final Map<String, Object> parameters) {
    super(graph, compileToGremlin(graph, cypherQuery, parameters));
  }

  @Override
  public ResultSet execute() {
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
        if (map.size() == 1 && nextValue instanceof Map) {
          result.addAll(CypherQueryEngine.transformMap((Map<?, ?>) nextValue));
        } else {
          result.addAll(CypherQueryEngine.transformMap(map));
        }
      }
    }
    return result;
  }

  public static String compileToGremlin(final ArcadeGraph graph, final String cypher, final Map<String, Object> parameters) {
    if (CACHE_SIZE == 0)
      // NO CACHE
      return parameters != null ? new TranslationFacade().toGremlinGroovy(cypher, parameters) : new TranslationFacade().toGremlinGroovy(cypher);

    final String db = graph.getDatabase().getDatabasePath();

    final String mapKey = db + ":" + cypher;

    final CachedStatement cached = STATEMENT_CACHE.get(mapKey);
    // FOUND
    if (cached != null) {
      ++cached.used;
      return cached.gremlin;
    }

    // TRANSLATE TO GREMLIN AND CACHE THE STATEMENT FOR FURTHER USAGE
    final CypherAst ast = parameters == null ? CypherAst.parse(cypher) : CypherAst.parse(cypher, parameters);
    final Translator<String, GroovyPredicate> translator = Translator.builder().gremlinGroovy().enableCypherExtensions().build();
    String gremlin = ast.buildTranslation(translator);

    // REPLACE '  cypher.null' WITH NULL
    gremlin = gremlin.replace("'  cypher.null'", "null");

    cacheLastStatement(cypher, gremlin);

    return gremlin;
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
