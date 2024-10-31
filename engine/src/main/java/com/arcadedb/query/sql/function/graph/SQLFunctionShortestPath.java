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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeToVertexIterable;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.function.math.SQLFunctionMathAbstract;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionShortestPath extends SQLFunctionMathAbstract {
  public static final String NAME            = "shortestPath";
  public static final String PARAM_MAX_DEPTH = "maxDepth";

  public SQLFunctionShortestPath() {
    super(NAME);
  }

  private static class ShortestPathContext {
    Vertex           sourceVertex;
    Vertex           destinationVertex;
    Vertex.DIRECTION directionLeft  = Vertex.DIRECTION.BOTH;
    Vertex.DIRECTION directionRight = Vertex.DIRECTION.BOTH;

    String   edgeType;
    String[] edgeTypeParam;

    ArrayDeque<Vertex> queueLeft  = new ArrayDeque<>();
    ArrayDeque<Vertex> queueRight = new ArrayDeque<>();

    final Set<RID>      leftVisited  = new HashSet<>();
    final Set<RID>      rightVisited = new HashSet<>();
    final Map<RID, RID> previouses   = new HashMap<>();
    final Map<RID, RID> nexts        = new HashMap<>();

    Vertex current;
    Vertex currentRight;

    public Integer maxDepth;
    /**
     * option that decides whether or not to return the edge information
     */
    public Boolean edge;
  }

  public List<RID> execute(final Object iThis, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {

    final ShortestPathContext shortestPathContext = new ShortestPathContext();

    Object source = params[0];
    if (MultiValue.isMultiValue(source)) {
      if (MultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = MultiValue.getFirstValue(source);
      if (source instanceof Result result && result.isElement()) {
        source = result.getElement().get();
      }
    }

    //    source = record.get((String) source);

    if (source instanceof Identifiable identifiable) {
      final Document elem = (Document) identifiable.getRecord();
      if (!(elem instanceof Vertex))
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");

      shortestPathContext.sourceVertex = (Vertex) elem;
    } else {
      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
    }

    Object dest = params[1];
    if (MultiValue.isMultiValue(dest)) {
      if (MultiValue.getSize(dest) > 1)
        throw new IllegalArgumentException("Only one destinationVertex is allowed");
      dest = MultiValue.getFirstValue(dest);
      if (dest instanceof Result result && result.isElement()) {
        dest = result.getElement().get();
      }
    }

    //    dest = record.get((String) dest);

    if (dest instanceof Identifiable identifiable1) {
      final Document elem = (Document) identifiable1.getRecord();
      if (!(elem instanceof Vertex vertex))
        throw new IllegalArgumentException("The destinationVertex must be a vertex record");

      shortestPathContext.destinationVertex = vertex;
    } else {
      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
    }

    if (shortestPathContext.sourceVertex.equals(shortestPathContext.destinationVertex)) {
      final List<RID> result = new ArrayList<>(1);
      result.add(shortestPathContext.destinationVertex.getIdentity());
      return result;
    }

    if (params.length > 2 && params[2] != null) {
      shortestPathContext.directionLeft = Vertex.DIRECTION.valueOf(params[2].toString().toUpperCase(Locale.ENGLISH));
    }
    if (shortestPathContext.directionLeft == Vertex.DIRECTION.OUT) {
      shortestPathContext.directionRight = Vertex.DIRECTION.IN;
    } else if (shortestPathContext.directionLeft == Vertex.DIRECTION.IN) {
      shortestPathContext.directionRight = Vertex.DIRECTION.OUT;
    }

    shortestPathContext.edgeType = null;
    if (params.length > 3) {
      shortestPathContext.edgeType = params[3] == null ? null : "" + params[3];
    }
    shortestPathContext.edgeTypeParam = null;
    if (params.length > 3 && params[3] != null) {
      if (params[3] instanceof List<?> list) {
        shortestPathContext.edgeTypeParam = list.toArray(new String[0]);
      } else
        shortestPathContext.edgeTypeParam = new String[] { shortestPathContext.edgeType };
    }

    if (params.length > 4) {
      bindAdditionalParams(params[4], shortestPathContext);
    }

    shortestPathContext.queueLeft.add(shortestPathContext.sourceVertex);
    shortestPathContext.leftVisited.add(shortestPathContext.sourceVertex.getIdentity());

    shortestPathContext.queueRight.add(shortestPathContext.destinationVertex);
    shortestPathContext.rightVisited.add(shortestPathContext.destinationVertex.getIdentity());

    int depth = 1;
    while (true) {
      if (shortestPathContext.maxDepth != null && shortestPathContext.maxDepth <= depth) {
        break;
      }
      if (shortestPathContext.queueLeft.isEmpty() || shortestPathContext.queueRight.isEmpty())
        break;

      if (Thread.interrupted())
        throw new CommandExecutionException("The shortestPath() function has been interrupted");

      List<RID> neighborIdentity;

      if (shortestPathContext.queueLeft.size() <= shortestPathContext.queueRight.size()) {
        // START EVALUATING FROM LEFT
        neighborIdentity = walkLeft(shortestPathContext);
        if (neighborIdentity != null)
          return neighborIdentity;
        depth++;
        if (shortestPathContext.maxDepth != null && shortestPathContext.maxDepth <= depth) {
          break;
        }

        if (shortestPathContext.queueLeft.isEmpty())
          break;

        neighborIdentity = walkRight(shortestPathContext);
        if (neighborIdentity != null)
          return neighborIdentity;

      } else {
        // START EVALUATING FROM RIGHT
        neighborIdentity = walkRight(shortestPathContext);
        if (neighborIdentity != null)
          return neighborIdentity;

        depth++;
        if (shortestPathContext.maxDepth != null && shortestPathContext.maxDepth <= depth) {
          break;
        }

        if (shortestPathContext.queueRight.isEmpty())
          break;

        neighborIdentity = walkLeft(shortestPathContext);
        if (neighborIdentity != null)
          return neighborIdentity;
      }

      depth++;
    }
    return new ArrayList<>();

  }

  private void bindAdditionalParams(final Object additionalParams, final ShortestPathContext context) {
    if (additionalParams == null)
      return;

    Map<String, Object> mapParams = null;
    if (additionalParams instanceof Map map)
      mapParams = map;
    else if (additionalParams instanceof Identifiable identifiable)
      mapParams = identifiable.getRecord().asDocument().toMap();

    if (mapParams != null) {
      context.maxDepth = integer(mapParams.get("maxDepth"));
      final Boolean withEdge = toBoolean(mapParams.get("edge"));
      context.edge = Boolean.TRUE.equals(withEdge) ? Boolean.TRUE : Boolean.FALSE;
    }
  }

  private Integer integer(final Object fromObject) {
    if (fromObject == null)
      return null;

    if (fromObject instanceof Number number)
      return number.intValue();

    if (fromObject instanceof String string) {
      try {
        return Integer.parseInt(string);
      } catch (final NumberFormatException ignore) {
      }
    }
    return null;
  }

  /**
   * @return
   *
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private Boolean toBoolean(final Object fromObject) {
    if (fromObject == null)
      return null;

    if (fromObject instanceof Boolean bool)
      return bool;

    if (fromObject instanceof String string) {
      try {
        return Boolean.parseBoolean(string);
      } catch (final NumberFormatException ignore) {
      }
    }
    return null;
  }

  /**
   * get adjacent vertices and edges
   *
   * @param srcVertex
   * @param direction
   * @param types
   *
   * @return
   *
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private Pair<Iterable<Vertex>, Iterable<Edge>> getVerticesAndEdges(final Vertex srcVertex, final Vertex.DIRECTION direction,
      final String... types) {
    if (direction == Vertex.DIRECTION.BOTH) {
      final MultiIterator<Vertex> vertexIterator = new MultiIterator<>();
      final MultiIterator<Edge> edgeIterator = new MultiIterator<>();
      final Pair<Iterable<Vertex>, Iterable<Edge>> pair1 = getVerticesAndEdges(srcVertex, Vertex.DIRECTION.OUT, types);
      final Pair<Iterable<Vertex>, Iterable<Edge>> pair2 = getVerticesAndEdges(srcVertex, Vertex.DIRECTION.IN, types);
      vertexIterator.addIterator(pair1.getFirst());
      vertexIterator.addIterator(pair2.getFirst());
      edgeIterator.addIterator(pair1.getSecond());
      edgeIterator.addIterator(pair2.getSecond());
      return new Pair<>(vertexIterator, edgeIterator);
    } else {
      final Iterable<Edge> edges1 = srcVertex.getEdges(direction, types);
      final Iterable<Edge> edges2 = srcVertex.getEdges(direction, types);
      return new Pair<>(new EdgeToVertexIterable(edges1, direction), edges2);
    }
  }

  /**
   * get adjacent vertices and edges
   *
   * @param srcVertex
   * @param direction
   *
   * @return
   *
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private Pair<Iterable<Vertex>, Iterable<Edge>> getVerticesAndEdges(final Vertex srcVertex, final Vertex.DIRECTION direction) {
    return getVerticesAndEdges(srcVertex, direction, (String[]) null);
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
  }

  protected List<RID> walkLeft(final ShortestPathContext context) {
    final ArrayDeque<Vertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(context.edge)) {
      while (!context.queueLeft.isEmpty()) {
        context.current = context.queueLeft.poll();

        final Iterable<Vertex> neighbors;
        if (context.edgeType == null) {
          neighbors = context.current.getVertices(context.directionLeft);
        } else {
          neighbors = context.current.getVertices(context.directionLeft, context.edgeTypeParam);
        }
        for (final Vertex neighbor : neighbors) {
          final RID neighborIdentity = neighbor.getIdentity();

          if (context.rightVisited.contains(neighborIdentity)) {
            context.previouses.put(neighborIdentity, context.current.getIdentity());
            return computePath(context.previouses, context.nexts, neighborIdentity);
          }
          if (!context.leftVisited.contains(neighborIdentity)) {
            context.previouses.put(neighborIdentity, context.current.getIdentity());

            nextLevelQueue.offer(neighbor);
            context.leftVisited.add(neighborIdentity);
          }

        }
      }
    } else {
      while (!context.queueLeft.isEmpty()) {
        context.current = context.queueLeft.poll();

        final Pair<Iterable<Vertex>, Iterable<Edge>> neighbors;
        if (context.edgeType == null) {
          neighbors = getVerticesAndEdges(context.current, context.directionLeft);
        } else {
          neighbors = getVerticesAndEdges(context.current, context.directionLeft, context.edgeTypeParam);
        }
        final Iterator<Vertex> vertexIterator = neighbors.getFirst().iterator();
        final Iterator<Edge> edgeIterator = neighbors.getSecond().iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          final Vertex v = vertexIterator.next();
          final RID neighborVertexIdentity = v.getIdentity();
          final RID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (context.rightVisited.contains(neighborVertexIdentity)) {
            context.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            context.previouses.put(neighborEdgeIdentity, context.current.getIdentity());
            return computePath(context.previouses, context.nexts, neighborVertexIdentity);
          }
          if (!context.leftVisited.contains(neighborVertexIdentity)) {
            context.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            context.previouses.put(neighborEdgeIdentity, context.current.getIdentity());

            nextLevelQueue.offer(v);
            context.leftVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    context.queueLeft = nextLevelQueue;
    return null;
  }

  protected List<RID> walkRight(final ShortestPathContext context) {
    final ArrayDeque<Vertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(context.edge)) {
      while (!context.queueRight.isEmpty()) {
        context.currentRight = context.queueRight.poll();

        final Iterable<Vertex> neighbors;
        if (context.edgeType == null) {
          neighbors = context.currentRight.getVertices(context.directionRight);
        } else {
          neighbors = context.currentRight.getVertices(context.directionRight, context.edgeTypeParam);
        }
        for (final Vertex neighbor : neighbors) {
          final RID neighborIdentity = neighbor.getIdentity();

          if (context.leftVisited.contains(neighborIdentity)) {
            context.nexts.put(neighborIdentity, context.currentRight.getIdentity());
            return computePath(context.previouses, context.nexts, neighborIdentity);
          }
          if (!context.rightVisited.contains(neighborIdentity)) {

            context.nexts.put(neighborIdentity, context.currentRight.getIdentity());

            nextLevelQueue.offer(neighbor);
            context.rightVisited.add(neighborIdentity);
          }

        }
      }
    } else {
      while (!context.queueRight.isEmpty()) {
        context.currentRight = context.queueRight.poll();

        final Pair<Iterable<Vertex>, Iterable<Edge>> neighbors;
        if (context.edgeType == null) {
          neighbors = getVerticesAndEdges(context.currentRight, context.directionRight);
        } else {
          neighbors = getVerticesAndEdges(context.currentRight, context.directionRight, context.edgeTypeParam);
        }

        final Iterator<Vertex> vertexIterator = neighbors.getFirst().iterator();
        final Iterator<Edge> edgeIterator = neighbors.getSecond().iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          final Vertex v = vertexIterator.next();
          final RID neighborVertexIdentity = v.getIdentity();
          final RID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (context.leftVisited.contains(neighborVertexIdentity)) {
            context.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            context.nexts.put(neighborEdgeIdentity, context.currentRight.getIdentity());
            return computePath(context.previouses, context.nexts, neighborVertexIdentity);
          }
          if (!context.rightVisited.contains(neighborVertexIdentity)) {
            context.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            context.nexts.put(neighborEdgeIdentity, context.currentRight.getIdentity());

            nextLevelQueue.offer(v);
            context.rightVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    context.queueRight = nextLevelQueue;
    return null;
  }

  private List<RID> computePath(final Map<RID, RID> leftDistances, final Map<RID, RID> rightDistances, final RID neighbor) {
    final List<RID> result = new ArrayList<RID>();

    RID current = neighbor;
    while (current != null) {
      result.add(0, current);
      current = leftDistances.get(current);
    }

    current = neighbor;
    while (current != null) {
      current = rightDistances.get(current);
      if (current != null) {
        result.add(current);
      }
    }

    return result;
  }
}
