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
package com.arcadedb.cypher.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.cypher.ArcadeCypher;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.arcadedb.schema.Property.IN_PROPERTY;
import static com.arcadedb.schema.Property.OUT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

public class CypherQueryEngine implements QueryEngine {
  static final  String      ENGINE_NAME = "cypher";
  private final ArcadeGraph arcadeGraph;

  protected CypherQueryEngine(final ArcadeGraph arcadeGraph) {
    this.arcadeGraph = arcadeGraph;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    ArcadeCypher cypher = arcadeGraph.cypher(query);

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
  public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    return command(query, configuration, parameters);
  }

  @Override
  public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
    return command(query, null, parameters);
  }

  @Override
  public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
    try {
      final ArcadeCypher arcadeCypher = arcadeGraph.cypher(query);
      arcadeCypher.setParameters(parameters);
      return arcadeCypher.execute();

    } catch (final Exception e) {
      throw new CommandParsingException("Error on executing Cypher query", e);
    }
  }

  public static Object transformValue(final Object value, final boolean flatArrays) {
    if (value instanceof Map<?, ?> map) {
      final List<ResultInternal> list = transformMap(map);
      if (list.size() == 1)
        return list.getFirst();
      return list;
    } else if (value instanceof List<?> listValue) {
      final List<Object> transformed = listValue.stream().map(value1 -> transformValue(value1, false)).collect(Collectors.toList());
      return flatArrays && transformed.size() == 1 ? transformed.getFirst() : transformed;
    }
    return value;
  }

  public static List<ResultInternal> transformMap(final Map<?, ?> map) {

    final List<ResultInternal> result = new ArrayList<>();

    final Map<String, Object> mapStringObject = new LinkedHashMap<>(map.size());
    if (map.containsKey("  cypher.element")) {
      final Object in = map.get("  cypher.inv");
      if (in != null)
        mapStringObject.put(IN_PROPERTY, in);

      final Object out = map.get("  cypher.outv");
      if (out != null)
        mapStringObject.put(OUT_PROPERTY, out);

      final Object element = map.get("  cypher.element");
      if (element instanceof Map map1)
        result.add(cypherObjectToResult(mapStringObject, map1));
      else if (element instanceof List) {
        final List<Map> list = (List<Map>) element;
        for (final Map mapEntry : list)
          result.add(cypherObjectToResult(new LinkedHashMap<>(), mapEntry));
      }
    } else
      result.add(cypherObjectToResult(mapStringObject, new LinkedHashMap<>(map)));

    return result;
  }

  private static ResultInternal cypherObjectToResult(final Map<String, Object> mapStringObject,
      final Map<Object, Object> internalMap) {
    boolean isAnObject = false;
    for (final Map.Entry<Object, Object> entry : internalMap.entrySet()) {
      Object mapKey = entry.getKey();
      Object mapValue = entry.getValue();

      if (mapKey.getClass().getName().startsWith("org.apache.tinkerpop.gremlin.structure.T$")) {
        isAnObject = true;
        mapKey = switch (mapKey.toString()) {
          case "id" -> RID_PROPERTY;
          case "label" -> TYPE_PROPERTY;
          default -> mapKey;
        };
      } else if (mapKey.equals("  cypher.element")) {
      } else {
        mapValue = transformValue(mapValue, isAnObject);
      }
      mapStringObject.put(mapKey.toString(), mapValue);
    }

    return new ResultInternal(mapStringObject);
  }

  @Override
  public ResultSet command(final String query, ContextConfiguration configuration, final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    final Map<String, Object> map = new LinkedHashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, null, map);
  }

  @Override
  public void close() {
    if (arcadeGraph != null)
      arcadeGraph.close();
  }
}
