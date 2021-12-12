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
 */
package com.arcadedb.query.cypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.lang.reflect.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

public class CypherQueryEngine implements QueryEngine {
  private static final String ENGINE_NAME = "cypher-engine";
  private final        Object arcadeGraph;

  public static class CypherQueryEngineFactory implements QueryEngineFactory {
    private static Boolean  available = null;
    private static Class<?> arcadeGraphClass;
    private static Class<?> arcadeCypherClass;

    @Override
    public boolean isAvailable() {
      if (available == null) {
        try {
          arcadeGraphClass = Class.forName("org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph");
          arcadeCypherClass = Class.forName("org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeCypher");
          available = true;
        } catch (ClassNotFoundException e) {
          available = false;
        }
      }
      return available;
    }

    @Override
    public String getLanguage() {
      return "cypher";
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      Object engine = database.getWrappers().get(ENGINE_NAME);
      if (engine != null)
        return (CypherQueryEngine) engine;

      try {
        engine = new CypherQueryEngine(arcadeGraphClass.getMethod("open", Database.class).invoke(null, database));
        database.setWrapper(ENGINE_NAME, engine);
        return (CypherQueryEngine) engine;

      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on initializing Cypher query engine", e);
        throw new QueryParsingException("Error on initializing Cypher query engine", e);
      }
    }
  }

  protected CypherQueryEngine(final Object arcadeGraph) {
    this.arcadeGraph = arcadeGraph;
  }

  @Override
  public AnalyzedQuery analyze(String query) {
    return new AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return false;
      }

      @Override
      public boolean isDDL() {
        return false;
      }
    };
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> parameters) {
    return command(query, parameters);
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    return command(query, parameters);
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    try {
      final Object arcadeGremlin = CypherQueryEngineFactory.arcadeGraphClass.getMethod("cypher", String.class).invoke(arcadeGraph, query);
      CypherQueryEngineFactory.arcadeCypherClass.getMethod("setParameters", Map.class).invoke(arcadeGremlin, parameters);
      return (ResultSet) CypherQueryEngineFactory.arcadeCypherClass.getMethod("execute").invoke(arcadeGremlin);

    } catch (InvocationTargetException e) {
      throw new CommandExecutionException("Error on executing cypher command", e.getTargetException());
    } catch (Exception e) {
      throw new QueryParsingException("Error on executing Cypher query", e);
    }
  }

  public static Object transformValue(Object value) {
    if (value instanceof Map) {
      return new ResultInternal(transformMap((Map<?, ?>) value));
    } else if (value instanceof List) {
      List<?> listValue = (List<?>) value;
      List<Object> transformed = listValue.stream().map(CypherQueryEngine::transformValue).collect(Collectors.toList());
      return transformed.size() == 1 ? transformed.iterator().next() : transformed;
    }
    return value;
  }

  public static Map<String, Object> transformMap(Map<? extends Object, ? extends Object> map) {

    final Map<String, Object> mapStringObject = new HashMap<>(map.size());
    Map<Object, Object> internal = new HashMap<>(map);
    if (map.containsKey("  cypher.element")) {
      mapStringObject.put("@in", map.get("  cypher.inv"));
      mapStringObject.put("@out", map.get("  cypher.outv"));
      internal = (Map) map.get("  cypher.element");
    }

    for (Map.Entry<Object, Object> entry : internal.entrySet()) {
      Object mapKey = entry.getKey();
      Object mapValue = entry.getValue();

      if (mapKey.getClass().getName().startsWith("org.apache.tinkerpop.gremlin.structure.T$")) {
        switch (mapKey.toString()) {
        case "id":
          mapKey = "@rid";
          break;
        case "label":
          mapKey = "@type";
          break;
        }
      } else if (mapKey.equals("  cypher.element")) {
      } else {
        mapValue = transformValue(mapValue);
      }
      mapStringObject.put(mapKey.toString(), mapValue);
    }
    return mapStringObject;
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, map);
  }
}
