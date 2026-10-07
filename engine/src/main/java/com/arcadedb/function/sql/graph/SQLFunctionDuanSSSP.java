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
package com.arcadedb.function.sql.graph;

import com.arcadedb.graph.EdgeWeight;

/**
 * {@code duanSSSP(<sourceVertex>, <destinationVertex>, [<weightEdgeFieldName>], [<direction> | { direction, edgeTypeNames }])}:
 * the weighted point-to-point shortest path, named after Duan et al., "Breaking the Sorting Barrier for Directed
 * Single-Source Shortest Paths" (2025, https://arxiv.org/abs/2504.17033).
 * <p>
 * The paper's algorithm has constant factors too large to win at practical graph sizes, and this function never ran it:
 * it was a plain Dijkstra over the edge records with a weight rule of its own and no edge type filter. Since issue #9443
 * it answers through the same engine as {@code cchShortestPath()} - a Customizable Contraction Hierarchy when a Graph
 * Analytical View keeps one, bidirectional Dijkstra on the view or on the records otherwise - so it shares their weight
 * rule ({@link EdgeWeight}), their edge type filter, their command timeout and their acceleration. The only difference
 * left is the weight property, optional here and {@code weight} by default.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionDuanSSSP extends SQLFunctionCCHShortestPath {
  public static final String NAME = "duanSSSP";

  public SQLFunctionDuanSSSP() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getSyntax() {
    return "duanSSSP(<sourceVertex>, <destinationVertex>, [<weightEdgeFieldName>], [<direction> | { direction, edgeTypeNames }])";
  }
}
