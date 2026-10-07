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
package com.arcadedb.graph;

/**
 * The one definition of what an edge weighs for the weighted shortest-path finders: {@code dijkstra()}, {@code astar()},
 * {@code duanSSSP()}, {@code cchShortestPath()} and the {@code algo.dijkstra}, {@code algo.astar},
 * {@code algo.dijkstra.singleSource}, {@code algo.cch.shortestPath}, {@code algo.kShortestPaths} and
 * {@code algo.steinerTree} procedures (issue #9443).
 * <ul>
 *   <li>an edge without the weight property, or whose value is not a number, weighs {@link #MISSING}: the unweighted
 *       hop. Counting it 0 made every unweighted edge free;</li>
 *   <li>an edge whose value is negative, NaN or infinite is not walked. Dijkstra and A* settle a vertex for good the
 *       first time they reach it, so a negative weight met afterwards produces an arbitrary path rather than a shortest
 *       one. Negative weights are the business of {@code bellmanFord()}, which keeps reading the raw value.</li>
 * </ul>
 * Before this the finders disagreed on both points, so the same query answered different distances and paths depending
 * on the entry point.
 * <p>
 * Always pair {@link #of(Object)} with {@link #isWalkable(double)}: {@link #NOT_WALKABLE} is a negative sentinel, so a
 * caller that adds the value without the check walks the edge at a negative cost. Weights read from a view's columns
 * never pass through {@link #of(Object)} and need the same check on the raw value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class EdgeWeight {
  /** What an edge without a numeric weight weighs. */
  public static final double MISSING      = 1.0;
  /** What {@link #of(Object)} answers for an edge that must not be walked; never {@link #isWalkable walkable}. */
  public static final double NOT_WALKABLE = -1.0;

  private EdgeWeight() {
  }

  /**
   * The weight of an edge whose weight property holds {@code value}.
   *
   * @return {@link #MISSING} when {@code value} is not a number, {@link #NOT_WALKABLE} when it is negative, NaN or
   * infinite, the value itself otherwise
   */
  public static double of(final Object value) {
    if (!(value instanceof Number number))
      return MISSING;
    final double weight = number.doubleValue();
    return isWalkable(weight) ? weight : NOT_WALKABLE;
  }

  /** True when an edge of this weight can be part of a shortest path: a finite, non-negative number. */
  public static boolean isWalkable(final double weight) {
    // NaN fails both comparisons
    return weight >= 0 && weight < Double.POSITIVE_INFINITY;
  }
}
