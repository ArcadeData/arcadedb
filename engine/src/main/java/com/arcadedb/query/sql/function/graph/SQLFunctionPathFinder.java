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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.math.SQLFunctionMathAbstract;

import java.util.*;

/**
 * Abstract class to find paths between nodes.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionPathFinder extends SQLFunctionMathAbstract {
  protected Set<Vertex>      unSettledNodes;
  protected Map<RID, Vertex> predecessors;
  protected Map<RID, Float>  distance;

  protected Vertex           paramSourceVertex;
  protected Vertex           paramDestinationVertex;
  protected Vertex.DIRECTION paramDirection = Vertex.DIRECTION.OUT;
  protected CommandContext   context;

  protected static final float MIN = 0f;

  public SQLFunctionPathFinder(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected LinkedList<Vertex> execute(final CommandContext iContext) {
    context = iContext;
    unSettledNodes = new HashSet<Vertex>();
    distance = new HashMap<RID, Float>();
    predecessors = new HashMap<RID, Vertex>();
    distance.put(paramSourceVertex.getIdentity(), MIN);
    unSettledNodes.add(paramSourceVertex);

    int maxDistances = 0;
    int maxSettled = 0;
    int maxUnSettled = 0;
    int maxPredecessors = 0;

    while (continueTraversing()) {
      final Vertex node = getMinimum(unSettledNodes);
      unSettledNodes.remove(node);
      findMinimalDistances(node);

      if (distance.size() > maxDistances)
        maxDistances = distance.size();
      if (unSettledNodes.size() > maxUnSettled)
        maxUnSettled = unSettledNodes.size();
      if (predecessors.size() > maxPredecessors)
        maxPredecessors = predecessors.size();

      if (!isVariableEdgeWeight() && distance.containsKey(paramDestinationVertex.getIdentity()))
        // FOUND
        break;
    }

    context.setVariable("maxDistances", maxDistances);
    context.setVariable("maxSettled", maxSettled);
    context.setVariable("maxUnSettled", maxUnSettled);
    context.setVariable("maxPredecessors", maxPredecessors);

    distance = null;

    return getPath();
  }

  protected boolean isVariableEdgeWeight() {
    return false;
  }

  /*
   * This method returns the path from the source to the selected target and NULL if no path exists
   */
  public LinkedList<Vertex> getPath() {
    final LinkedList<Vertex> path = new LinkedList<Vertex>();
    Vertex step = paramDestinationVertex;
    // Check if a path exists
    if (predecessors.get(step.getIdentity()) == null)
      return null;

    path.add(step);
    while (predecessors.get(step.getIdentity()) != null) {
      step = predecessors.get(step.getIdentity());
      path.add(step);
    }
    // Put it into the correct order
    Collections.reverse(path);
    return path;
  }

  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  protected void findMinimalDistances(final Vertex node) {
    for (Vertex neighbor : getNeighbors(node)) {
      final float d = sumDistances(getShortestDistance(node), getDistance(node, neighbor));

      if (getShortestDistance(neighbor) > d) {
        distance.put(neighbor.getIdentity(), d);
        predecessors.put(neighbor.getIdentity(), node);
        unSettledNodes.add(neighbor);
      }
    }

  }

  protected Set<Vertex> getNeighbors(final Vertex node) {
    context.incrementVariable("getNeighbors");

    final Set<Vertex> neighbors = new HashSet<Vertex>();
    if (node != null) {
      for (Vertex v : node.getVertices(paramDirection)) {
        final Vertex ov = v;
        if (ov != null && isNotSettled(ov))
          neighbors.add(ov);
      }
    }
    return neighbors;
  }

  protected Vertex getMinimum(final Set<Vertex> vertexes) {
    Vertex minimum = null;
    Float minimumDistance = null;
    for (Vertex vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex) < minimumDistance) {
        minimum = vertex;
        minimumDistance = getShortestDistance(minimum);
      }
    }
    return minimum;
  }

  protected boolean isNotSettled(final Vertex vertex) {
    return unSettledNodes.contains(vertex) || !distance.containsKey(vertex.getIdentity());
  }

  protected boolean continueTraversing() {
    return unSettledNodes.size() > 0;
  }

  protected float getShortestDistance(final Vertex destination) {
    if (destination == null)
      return Float.MAX_VALUE;

    final Float d = distance.get(destination.getIdentity());
    return d == null ? Float.MAX_VALUE : d;
  }

  protected float sumDistances(final float iDistance1, final float iDistance2) {
    return iDistance1 + iDistance2;
  }

  protected abstract float getDistance(final Vertex node, final Vertex target);
}
