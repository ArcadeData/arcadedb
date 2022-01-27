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

import java.util.*;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionShortestPath extends SQLFunctionMathAbstract {
  public static final String NAME            = "shortestPath";
  public static final String PARAM_MAX_DEPTH = "maxDepth";

  protected static final float DISTANCE = 1f;

  public SQLFunctionShortestPath() {
    super(NAME, 2, 5);
  }

  private static class OShortestPathContext {
    Vertex           sourceVertex;
    Vertex           destinationVertex;
    Vertex.DIRECTION directionLeft  = Vertex.DIRECTION.BOTH;
    Vertex.DIRECTION directionRight = Vertex.DIRECTION.BOTH;

    String   edgeType;
    String[] edgeTypeParam;

    ArrayDeque<Vertex> queueLeft  = new ArrayDeque<>();
    ArrayDeque<Vertex> queueRight = new ArrayDeque<>();

    final Set<RID> leftVisited  = new HashSet<RID>();
    final Set<RID> rightVisited = new HashSet<RID>();

    final Map<RID, RID> previouses = new HashMap<RID, RID>();
    final Map<RID, RID> nexts      = new HashMap<RID, RID>();

    Vertex current;
    Vertex currentRight;
    public Integer maxDepth;
    /**
     * option that decides whether or not to return the edge information
     */
    public Boolean edge;
  }

  public List<RID> execute(Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {

    final OShortestPathContext ctx = new OShortestPathContext();

    Object source = iParams[0];
    if (MultiValue.isMultiValue(source)) {
      if (MultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = MultiValue.getFirstValue(source);
      if (source instanceof Result && ((Result) source).isElement()) {
        source = ((Result) source).getElement().get();
      }
    }

    //    source = record.get((String) source);

    if (source instanceof Identifiable) {
      final Document elem = (Document) ((Identifiable) source).getRecord();
      if (!(elem instanceof Vertex))
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");

      ctx.sourceVertex = (Vertex) elem;
    } else {
      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
    }

    Object dest = iParams[1];
    if (MultiValue.isMultiValue(dest)) {
      if (MultiValue.getSize(dest) > 1)
        throw new IllegalArgumentException("Only one destinationVertex is allowed");
      dest = MultiValue.getFirstValue(dest);
      if (dest instanceof Result && ((Result) dest).isElement()) {
        dest = ((Result) dest).getElement().get();
      }
    }

    //    dest = record.get((String) dest);

    if (dest instanceof Identifiable) {
      Document elem = (Document) ((Identifiable) dest).getRecord();
      if (!(elem instanceof Vertex))
        throw new IllegalArgumentException("The destinationVertex must be a vertex record");

      ctx.destinationVertex = (Vertex) elem;
    } else {
      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
    }

    if (ctx.sourceVertex.equals(ctx.destinationVertex)) {
      final List<RID> result = new ArrayList<RID>(1);
      result.add(ctx.destinationVertex.getIdentity());
      return result;
    }

    if (iParams.length > 2 && iParams[2] != null) {
      ctx.directionLeft = Vertex.DIRECTION.valueOf(iParams[2].toString().toUpperCase(Locale.ENGLISH));
    }
    if (ctx.directionLeft == Vertex.DIRECTION.OUT) {
      ctx.directionRight = Vertex.DIRECTION.IN;
    } else if (ctx.directionLeft == Vertex.DIRECTION.IN) {
      ctx.directionRight = Vertex.DIRECTION.OUT;
    }

    ctx.edgeType = null;
    if (iParams.length > 3) {
      ctx.edgeType = iParams[3] == null ? null : "" + iParams[3];
    }
    ctx.edgeTypeParam = null;
    if (iParams.length > 3 && iParams[3] != null) {
      if (iParams[3] instanceof List) {
        final List<String> list = (List<String>) iParams[3];
        ctx.edgeTypeParam = list.toArray(new String[list.size()]);
      } else
        ctx.edgeTypeParam = new String[] { ctx.edgeType };
    }

    if (iParams.length > 4) {
      bindAdditionalParams(iParams[4], ctx);
    }

    ctx.queueLeft.add(ctx.sourceVertex);
    ctx.leftVisited.add(ctx.sourceVertex.getIdentity());

    ctx.queueRight.add(ctx.destinationVertex);
    ctx.rightVisited.add(ctx.destinationVertex.getIdentity());

    int depth = 1;
    while (true) {
      if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
        break;
      }
      if (ctx.queueLeft.isEmpty() || ctx.queueRight.isEmpty())
        break;

      if (Thread.interrupted())
        throw new CommandExecutionException("The shortestPath() function has been interrupted");

      List<RID> neighborIdentity;

      if (ctx.queueLeft.size() <= ctx.queueRight.size()) {
        // START EVALUATING FROM LEFT
        neighborIdentity = walkLeft(ctx);
        if (neighborIdentity != null)
          return neighborIdentity;
        depth++;
        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
          break;
        }

        if (ctx.queueLeft.isEmpty())
          break;

        neighborIdentity = walkRight(ctx);
        if (neighborIdentity != null)
          return neighborIdentity;

      } else {

        // START EVALUATING FROM RIGHT
        neighborIdentity = walkRight(ctx);
        if (neighborIdentity != null)
          return neighborIdentity;

        depth++;
        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
          break;
        }

        if (ctx.queueRight.isEmpty())
          break;

        neighborIdentity = walkLeft(ctx);
        if (neighborIdentity != null)
          return neighborIdentity;
      }

      depth++;
    }
    return new ArrayList<RID>();

  }

  private void bindAdditionalParams(Object additionalParams, OShortestPathContext ctx) {
    if (additionalParams == null) {
      return;
    }
    Map<String, Object> mapParams = null;
    if (additionalParams instanceof Map) {
      mapParams = (Map) additionalParams;
    } else if (additionalParams instanceof Identifiable) {
      mapParams = ((Document) ((Identifiable) additionalParams).getRecord()).toMap();
    }
    if (mapParams != null) {
      ctx.maxDepth = integer(mapParams.get("maxDepth"));
      Boolean withEdge = toBoolean(mapParams.get("edge"));
      ctx.edge = Boolean.TRUE.equals(withEdge) ? Boolean.TRUE : Boolean.FALSE;
    }
  }

  private Integer integer(Object fromObject) {
    if (fromObject == null) {
      return null;
    }
    if (fromObject instanceof Number) {
      return ((Number) fromObject).intValue();
    }
    if (fromObject instanceof String) {
      try {
        return Integer.parseInt(fromObject.toString());
      } catch (NumberFormatException ignore) {
      }
    }
    return null;
  }

  /**
   * @return
   *
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private Boolean toBoolean(Object fromObject) {
    if (fromObject == null) {
      return null;
    }
    if (fromObject instanceof Boolean) {
      return (Boolean) fromObject;
    }
    if (fromObject instanceof String) {
      try {
        return Boolean.parseBoolean(fromObject.toString());
      } catch (NumberFormatException ignore) {
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
  private Pair<Iterable<Vertex>, Iterable<Edge>> getVerticesAndEdges(Vertex srcVertex, Vertex.DIRECTION direction, String... types) {
    if (direction == Vertex.DIRECTION.BOTH) {
      MultiIterator<Vertex> vertexIterator = new MultiIterator<>();
      MultiIterator<Edge> edgeIterator = new MultiIterator<>();
      Pair<Iterable<Vertex>, Iterable<Edge>> pair1 = getVerticesAndEdges(srcVertex, Vertex.DIRECTION.OUT, types);
      Pair<Iterable<Vertex>, Iterable<Edge>> pair2 = getVerticesAndEdges(srcVertex, Vertex.DIRECTION.IN, types);
      vertexIterator.addIterator(pair1.getFirst());
      vertexIterator.addIterator(pair2.getFirst());
      edgeIterator.addIterator(pair1.getSecond());
      edgeIterator.addIterator(pair2.getSecond());
      return new Pair<>(vertexIterator, edgeIterator);
    } else {
      Iterable<Edge> edges1 = srcVertex.getEdges(direction, types);
      Iterable<Edge> edges2 = srcVertex.getEdges(direction, types);
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
  private Pair<Iterable<Vertex>, Iterable<Edge>> getVerticesAndEdges(Vertex srcVertex, Vertex.DIRECTION direction) {
    return getVerticesAndEdges(srcVertex, direction, (String[]) null);
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
  }

  protected List<RID> walkLeft(final SQLFunctionShortestPath.OShortestPathContext ctx) {
    ArrayDeque<Vertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(ctx.edge)) {
      while (!ctx.queueLeft.isEmpty()) {
        ctx.current = ctx.queueLeft.poll();

        Iterable<Vertex> neighbors;
        if (ctx.edgeType == null) {
          neighbors = ctx.current.getVertices(ctx.directionLeft);
        } else {
          neighbors = ctx.current.getVertices(ctx.directionLeft, ctx.edgeTypeParam);
        }
        for (Vertex neighbor : neighbors) {
          final Vertex v = neighbor;
          final RID neighborIdentity = v.getIdentity();

          if (ctx.rightVisited.contains(neighborIdentity)) {
            ctx.previouses.put(neighborIdentity, ctx.current.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborIdentity);
          }
          if (!ctx.leftVisited.contains(neighborIdentity)) {
            ctx.previouses.put(neighborIdentity, ctx.current.getIdentity());

            nextLevelQueue.offer(v);
            ctx.leftVisited.add(neighborIdentity);
          }

        }
      }
    } else {
      while (!ctx.queueLeft.isEmpty()) {
        ctx.current = ctx.queueLeft.poll();

        Pair<Iterable<Vertex>, Iterable<Edge>> neighbors;
        if (ctx.edgeType == null) {
          neighbors = getVerticesAndEdges(ctx.current, ctx.directionLeft);
        } else {
          neighbors = getVerticesAndEdges(ctx.current, ctx.directionLeft, ctx.edgeTypeParam);
        }
        Iterator<Vertex> vertexIterator = neighbors.getFirst().iterator();
        Iterator<Edge> edgeIterator = neighbors.getSecond().iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          Vertex v = vertexIterator.next();
          final RID neighborVertexIdentity = v.getIdentity();
          final RID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (ctx.rightVisited.contains(neighborVertexIdentity)) {
            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
          }
          if (!ctx.leftVisited.contains(neighborVertexIdentity)) {
            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());

            nextLevelQueue.offer(v);
            ctx.leftVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    ctx.queueLeft = nextLevelQueue;
    return null;
  }

  protected List<RID> walkRight(final SQLFunctionShortestPath.OShortestPathContext ctx) {
    final ArrayDeque<Vertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(ctx.edge)) {
      while (!ctx.queueRight.isEmpty()) {
        ctx.currentRight = ctx.queueRight.poll();

        Iterable<Vertex> neighbors;
        if (ctx.edgeType == null) {
          neighbors = ctx.currentRight.getVertices(ctx.directionRight);
        } else {
          neighbors = ctx.currentRight.getVertices(ctx.directionRight, ctx.edgeTypeParam);
        }
        for (Vertex neighbor : neighbors) {
          final Vertex v = neighbor;
          final RID neighborIdentity = v.getIdentity();

          if (ctx.leftVisited.contains(neighborIdentity)) {
            ctx.nexts.put(neighborIdentity, ctx.currentRight.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborIdentity);
          }
          if (!ctx.rightVisited.contains(neighborIdentity)) {

            ctx.nexts.put(neighborIdentity, ctx.currentRight.getIdentity());

            nextLevelQueue.offer(v);
            ctx.rightVisited.add(neighborIdentity);
          }

        }
      }
    } else {
      while (!ctx.queueRight.isEmpty()) {
        ctx.currentRight = ctx.queueRight.poll();

        Pair<Iterable<Vertex>, Iterable<Edge>> neighbors;
        if (ctx.edgeType == null) {
          neighbors = getVerticesAndEdges(ctx.currentRight, ctx.directionRight);
        } else {
          neighbors = getVerticesAndEdges(ctx.currentRight, ctx.directionRight, ctx.edgeTypeParam);
        }

        Iterator<Vertex> vertexIterator = neighbors.getFirst().iterator();
        Iterator<Edge> edgeIterator = neighbors.getSecond().iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          final Vertex v = vertexIterator.next();
          final RID neighborVertexIdentity = v.getIdentity();
          final RID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (ctx.leftVisited.contains(neighborVertexIdentity)) {
            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
          }
          if (!ctx.rightVisited.contains(neighborVertexIdentity)) {
            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());

            nextLevelQueue.offer(v);
            ctx.rightVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    ctx.queueRight = nextLevelQueue;
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
