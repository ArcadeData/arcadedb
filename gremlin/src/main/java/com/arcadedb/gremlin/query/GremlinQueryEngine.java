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
package com.arcadedb.gremlin.query;

import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.gremlin.ArcadeGremlin;

import java.util.*;

public class GremlinQueryEngine implements QueryEngine {
  static final  String      ENGINE_NAME = "gremlin";
  private final ArcadeGraph arcadeGraph;

  protected GremlinQueryEngine(final ArcadeGraph arcadeGraph) {
    this.arcadeGraph = arcadeGraph;
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
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
      final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);
      arcadeGremlin.setParameters(parameters);
      return arcadeGremlin.execute();
    } catch (final Exception e) {
      throw new QueryParsingException("Error on executing Gremlin query", e);
    }
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    final Map<String, Object> map = new HashMap<>(parameters.length / 2);
    for (int i = 0; i < parameters.length; i += 2)
      map.put((String) parameters[i], parameters[i + 1]);
    return command(query, map);
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      final ArcadeGremlin arcadeGremlin = arcadeGraph.gremlin(query);

      return arcadeGremlin.parse();

    } catch (final Exception e) {
      throw new QueryParsingException("Error on parsing Gremlin query", e);
    }
  }
}
