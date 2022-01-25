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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight'.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionDijkstra extends SQLFunctionPathFinder {
  public static final String NAME = "dijkstra";

  public SQLFunctionDijkstra() {
    super(NAME, 3, 4);
  }

  public LinkedList<Vertex> execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    return new SQLFunctionAstar().execute(this, iCurrentRecord, iCurrentResult, toAStarParams(iParams), iContext);
  }

  private Object[] toAStarParams(Object[] iParams) {
    Object[] result = new Object[4];
    result[0] = iParams[0];
    result[1] = iParams[1];
    result[2] = iParams[2];
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("emptyIfMaxDepth", true);
    if (iParams.length > 3) {
      options.put("direction", iParams[3]);
    }
    result[3] = options;
    return result;
  }

  public String getSyntax() {
    return "dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>])";
  }

  protected float getDistance(final Vertex node, final Vertex target) {
    return -1;//not used anymore
  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
