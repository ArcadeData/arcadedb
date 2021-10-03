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
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.lang.reflect.*;
import java.util.*;
import java.util.logging.*;

public class CypherQueryEngine implements QueryEngine {
  private final Object arcadeGraph;

  public static class CypherQueryEngineFactory implements QueryEngineFactory {
    private static Boolean available = null;
    private static Class   arcadeGraphClass;
    private static Class   arcadeCypherClass;

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
    public QueryEngine create(final DatabaseInternal database) {
      try {
        return new CypherQueryEngine(arcadeGraphClass.getMethod("open", Database.class).invoke(null, database));
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
      final ResultSet resultSet = (ResultSet) CypherQueryEngineFactory.arcadeCypherClass.getMethod("execute").invoke(arcadeGremlin);

      final InternalResultSet result = new InternalResultSet();

      while (resultSet.hasNext()) {
        final Result next = resultSet.next();
        if (next.isElement())
          result.add(next);
        else {
          Result row;

          final Set<String> properties = next.getPropertyNames();

          if (properties.size() == 1 && next.getProperty(properties.iterator().next()) instanceof Map) {
            // ONLY ONE ELEMENT THAT IS A MAP: EXPAND THE MAP INTO A RECORD
            row = mapToResult(next.getProperty(properties.iterator().next()));
          } else {
            final Map<String, Object> mapStringObject = new HashMap<>(properties.size());
            for (String propertyName : properties) {
              Object propertyValue = next.getProperty(propertyName);
              if (propertyValue instanceof Map)
                propertyValue = mapToResult((Map<Object, Object>) propertyValue);
              mapStringObject.put(propertyName, propertyValue);
            }
            row = new ResultInternal(mapStringObject);
          }

          result.add(row);
        }
      }

      return result;

    } catch (InvocationTargetException e) {
      throw new CommandExecutionException("Error on executing cypher command", e.getTargetException());
    } catch (Exception e) {
      throw new QueryParsingException("Error on executing Cypher query", e);
    }
  }

  private Result mapToResult(Map<Object, Object> map) {
    final Map<String, Object> mapStringObject = new HashMap<>(map.size());

    if (map.containsKey("  cypher.element")) {
      mapStringObject.put("@in", map.get("  cypher.inv"));
      mapStringObject.put("@out", map.get("  cypher.outv"));
      map = (Map<Object, Object>) map.get("  cypher.element");
    }

    for (Map.Entry<Object, Object> entry : map.entrySet()) {
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
      } else if (mapValue instanceof List && ((List<?>) mapValue).size() == 1) {
        mapValue = ((List<?>) mapValue).get(0);
      }

      mapStringObject.put(mapKey.toString(), mapValue);
    }

    return new ResultInternal(mapStringObject);
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, map);
  }
}
