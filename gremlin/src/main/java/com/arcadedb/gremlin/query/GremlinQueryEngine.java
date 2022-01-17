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
package com.arcadedb.gremlin.query;

import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGremlin;

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
    } catch (Exception e) {
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

    } catch (Exception e) {
      throw new QueryParsingException("Error on parsing Gremlin query", e);
    }
  }
}
