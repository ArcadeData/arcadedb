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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A*'s algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph with husrestic
 * function.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight' and fourth parameter represents the map of options.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0 .
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class SQLFunctionAstar extends SQLFunctionHeuristicPathFinderAbstract {
  public static final String NAME = "astar";

  private static final Set<String> OPTIONS = Set.of(PARAM_DIRECTION, PARAM_EDGE_TYPE_NAMES, PARAM_VERTEX_AXIS_NAMES,
      PARAM_PARALLEL, PARAM_MAX_DEPTH, PARAM_EMPTY_IF_MAX_DEPTH, PARAM_TIE_BREAKER, PARAM_D_FACTOR, PARAM_HEURISTIC_FORMULA,
      PARAM_CUSTOM_HEURISTIC_FORMULA);

  private         String              paramWeightFieldName = "weight";
  private         long                currentDepth         = 0;
  protected final Set<Vertex>         closedSet            = new HashSet<Vertex>();
  protected final Map<Vertex, Vertex> cameFrom             = new HashMap<Vertex, Vertex>();

  protected final Map<Vertex, Double>   gScore = new HashMap<Vertex, Double>();
  protected final Map<Vertex, Double>   fScore = new HashMap<Vertex, Double>();
  protected final PriorityQueue<Vertex> open   = new PriorityQueue<Vertex>(1,
      (nodeA, nodeB) -> Double.compare(fScore.get(nodeA), fScore.get(nodeB)));

  public SQLFunctionAstar() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  public LinkedList<RID> execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext ctx) {
    context = ctx;
    final SQLFunctionAstar astar = this;

    final Document record = currentRecord != null ? (Document) currentRecord.getRecord() : null;

    Object source = params[0];
    if (MultiValue.isMultiValue(source)) {
      if (MultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = MultiValue.getFirstValue(source);
      if (source instanceof Result && ((Result) source).isElement()) {
        source = ((Result) source).getElement().get();
      }
    }

    if (record != null && source instanceof String)
      source = record.get((String) source);

    if (source instanceof Identifiable) {
      final Document elem = (Document) ((Identifiable) source).getRecord();
      if (!(elem instanceof Vertex))
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");

      paramSourceVertex = (Vertex) elem;
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

    if (record != null && dest instanceof String)
      dest = record.get((String) dest);

    if (dest instanceof Identifiable identifiable) {
      final Document elem = (Document) identifiable.getRecord();
      if (!(elem instanceof Vertex vertex)) {
        throw new IllegalArgumentException("The destinationVertex must be a vertex record");
      }
      paramDestinationVertex = vertex;
    } else {
      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
    }

    paramWeightFieldName = FileUtils.getStringContent(params[2]);

    if (params.length > 3) {
      bindAdditionalParams(params[3], astar);
    }
    ctx.setVariable("getNeighbors", 0);
    if (paramSourceVertex == null || paramDestinationVertex == null) {
      return new LinkedList<>();
    }
    return internalExecute(ctx, ctx.getDatabase());

  }

  private LinkedList<RID> internalExecute(final CommandContext ctx, final Database graph) {

    final Vertex start = paramSourceVertex;
    final Vertex goal = paramDestinationVertex;

    // Neither maxDepth (defaults to Long.MAX_VALUE) nor graph exhaustion is a bound the caller controls, so
    // arcadedb.command.timeout has to be consulted from inside this loop like every other graph-driven
    // algorithm (issue #6459) - astar()/dijkstra() previously had no timeout/interrupt check at all.
    final WorkGuard guard = WorkGuard.forCommand(ctx, NAME + "()");

    open.add(start);

    // The cost of going from start to start is zero.
    gScore.put(start, 0.0);
    // For the first node, that value is completely heuristic.
    fScore.put(start, getHeuristicCost(start, null, goal, ctx));

    while (!open.isEmpty()) {
      guard.check();

      Vertex current = open.poll();

      // we discussed about this feature in https://github.com/orientechnologies/orientdb/pull/6002#issuecomment-212492687
      if (paramEmptyIfMaxDepth && currentDepth >= paramMaxDepth) {
        route.clear(); // to ensure our result is empty
        return getPath();
      }
      // if start and goal vertex is equal so return current path from  cameFrom hash map
      if (current.getIdentity().equals(goal.getIdentity()) || currentDepth >= paramMaxDepth) {

        while (current != null) {
          route.add(0, current);
          current = cameFrom.get(current);
        }
        return getPath();
      }

      closedSet.add(current);

      // Try CSR + edge property columns first for O(1) neighbor + weight access
      final Map<Vertex, Double> neighborWeights = getNeighborWeightsCSR(current, ctx);
      for (final Map.Entry<Vertex, Double> entry : neighborWeights.entrySet()) {
        final Vertex neighbor = entry.getKey();
        // Ignore the neighbor which is already evaluated.
        if (closedSet.contains(neighbor))
          continue;
        // The distance from start to a neighbor
        final double tentative_gScore = gScore.get(current) + entry.getValue();
        final boolean contains = open.contains(neighbor);

        if (!contains || tentative_gScore < gScore.get(neighbor)) {
          gScore.put(neighbor, tentative_gScore);
          fScore.put(neighbor, tentative_gScore + getHeuristicCost(neighbor, current, goal, ctx));

          if (contains)
            open.remove(neighbor);
          open.offer(neighbor);
          cameFrom.put(neighbor, current);
        }
      }

      // Increment Depth Level
      currentDepth++;

    }

    return getPath();
  }

  private Vertex getNeighbor(final Vertex current, final Edge neighborEdge, final Database graph) {
    if (neighborEdge.getOut().equals(current.getIdentity())) {
      return toVertex(neighborEdge.getIn());
    }
    return toVertex(neighborEdge.getOut());
  }

  private Vertex toVertex(Identifiable outVertex) {
    if (outVertex == null)
      return null;

    if (!(outVertex instanceof Record))
      outVertex = outVertex.getRecord();

    return (Vertex) outVertex;
  }

  protected Set<Edge> getNeighborEdges(final Vertex node) {
    context.incrementVariable("getNeighbors");

    final Set<Edge> neighbors = new HashSet<Edge>();
    if (node != null) {
      for (final Edge v : node.getEdges(paramDirection, paramEdgeTypeNames)) {
        if (v != null)
          neighbors.add(v);
      }
    }
    return neighbors;
  }

  /**
   * Returns neighbor vertices with their edge weights using CSR + edge property columns when available.
   * Falls back to OLTP edge traversal when GAV doesn't have edge properties or doesn't cover the node.
   */
  protected Map<Vertex, Double> getNeighborWeightsCSR(final Vertex node, final CommandContext ctx) {
    final Map<Vertex, Double> result = new HashMap<>();
    if (node == null)
      return result;

    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(
        ctx.getDatabase(), paramEdgeTypeNames);
    if (provider != null) {
      final int nodeId = provider.getNodeId(node.getIdentity());
      // edgeWeightsOf() answers null unless the provider can serve THIS property for EVERY type in play, and it
      // is the only thing that pairs a CSR edge with its own weight correctly. Reading getNeighborIds and
      // getEdgeProperty side by side here got it wrong twice over (issue #6301): the neighbour list is merged and
      // sorted across types while the property column is per type, and a BOTH lookup has no column at all, so it
      // answered null for every edge and quietly priced the whole neighbourhood at MIN - free.
      final NodeEdgeWeights edges = nodeId >= 0 ?
          provider.edgeWeightsOf(nodeId, paramDirection, paramWeightFieldName, MIN, paramEdgeTypeNames) : null;
      if (edges != null) {
        final int[] neighborIds = edges.neighbors();
        final double[] weights = edges.weights();
        for (int i = 0; i < neighborIds.length; i++) {
          final RID neighborRid = provider.getRID(neighborIds[i]);
          if (neighborRid == null)
            continue;
          try {
            result.put(neighborRid.asVertex(), weights[i]);
          } catch (final Exception e) {
            // deleted vertex — skip
          }
        }
        return result;
      }
    }

    // OLTP fallback
    for (final Edge edge : node.getEdges(paramDirection, paramEdgeTypeNames)) {
      try {
        final Vertex neighbor = getNeighbor(node, edge, ctx.getDatabase());
        if (neighbor != null)
          result.put(neighbor, getDistance(edge));
      } catch (final RecordNotFoundException e) {
        GhostEdgeReporter.reportSkipped(e);
      }
    }
    return result;
  }

  private void bindAdditionalParams(final Object additionalParams, final SQLFunctionAstar astar) {
    if (additionalParams == null)
      return;

    final Map<?, ?> rawMap;
    if (additionalParams instanceof Map<?, ?> map)
      rawMap = map;
    else if (additionalParams instanceof Identifiable identifiable)
      rawMap = ((Document) identifiable.getRecord()).toMap();
    else
      return;

    final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);

    if (opts.containsKey(PARAM_EDGE_TYPE_NAMES))
      astar.paramEdgeTypeNames = stringArray(opts.get(PARAM_EDGE_TYPE_NAMES));
    if (opts.containsKey(PARAM_VERTEX_AXIS_NAMES))
      astar.paramVertexAxisNames = stringArray(opts.get(PARAM_VERTEX_AXIS_NAMES));

    if (opts.containsKey(PARAM_DIRECTION)) {
      final Object raw = opts.get(PARAM_DIRECTION);
      if (raw instanceof Vertex.DIRECTION direction)
        astar.paramDirection = direction;
      else
        astar.paramDirection = Vertex.DIRECTION.valueOf(raw.toString().toUpperCase(Locale.ENGLISH));
    }

    astar.paramParallel = opts.getBoolean(PARAM_PARALLEL, astar.paramParallel);
    astar.paramMaxDepth = opts.getLong(PARAM_MAX_DEPTH, astar.paramMaxDepth);
    astar.paramEmptyIfMaxDepth = opts.getBoolean(PARAM_EMPTY_IF_MAX_DEPTH, astar.paramEmptyIfMaxDepth);
    astar.paramTieBreaker = opts.getBoolean(PARAM_TIE_BREAKER, astar.paramTieBreaker);
    astar.paramDFactor = opts.getDouble(PARAM_D_FACTOR, astar.paramDFactor);

    if (opts.containsKey(PARAM_HEURISTIC_FORMULA)) {
      final Object raw = opts.get(PARAM_HEURISTIC_FORMULA);
      if (raw instanceof SQLHeuristicFormula formula)
        astar.paramHeuristicFormula = formula;
      else
        astar.paramHeuristicFormula = SQLHeuristicFormula.valueOf(raw.toString().toUpperCase(Locale.ENGLISH));
    }

    // The option the syntax advertises is now applied instead of being read into a field nothing consulted (issue
    // #6414). Supplying it selects CUSTOM, so a caller does not have to write the formula name twice; naming a
    // different formula alongside it is a contradiction rather than a precedence question, and says so.
    if (opts.containsKey(PARAM_CUSTOM_HEURISTIC_FORMULA)) {
      if (opts.containsKey(PARAM_HEURISTIC_FORMULA) && astar.paramHeuristicFormula != SQLHeuristicFormula.CUSTOM)
        throw new CommandSQLParsingException(
            "Options '" + PARAM_HEURISTIC_FORMULA + "' (" + astar.paramHeuristicFormula + ") and '"
                + PARAM_CUSTOM_HEURISTIC_FORMULA + "' conflict for function '" + NAME
                + "': a custom formula replaces the built-in one, so name only one of them");

      astar.bindCustomHeuristicFunction(opts.getString(PARAM_CUSTOM_HEURISTIC_FORMULA, null), context);
    } else if (astar.paramHeuristicFormula == SQLHeuristicFormula.CUSTOM)
      throw new CommandSQLParsingException(
          "Option '" + PARAM_HEURISTIC_FORMULA + "' of function '" + NAME + "' is CUSTOM but no '"
              + PARAM_CUSTOM_HEURISTIC_FORMULA + "' was given to name the function to call");
  }

  public String getSyntax() {
    return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,heuristicFormula:'MANHATTAN',customHeuristicFormula:'custom_Function_Name_here'  }"
        + "\n // customHeuristicFormula names a SQL function called as fn(currentVertex, parentVertex, targetVertex, sourceVertex, depth, dFactor) and returning h(n) as a number";
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  @Override
  protected double getDistance(final Vertex node, final Vertex parent, final Vertex target) {
    final Iterator<Edge> edges = node.getEdges(paramDirection).iterator();
    Edge e = null;
    while (edges.hasNext()) {
      final Edge next = edges.next();
      try {
        if (next.getOut().equals(target.getIdentity()) || next.getIn().equals(target.getIdentity())) {
          e = next;
          break;
        }
      } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e' here: 'e' is the Edge loop variable in this scope
        GhostEdgeReporter.reportSkipped(rnf);
      }
    }
    if (e != null) {
      final Object fieldValue = e.get(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float)
          return (Float) fieldValue;
        else if (fieldValue instanceof Number)
          return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  protected double getDistance(final Edge edge) {
    if (edge != null) {
      final Object fieldValue = edge.get(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float)
          return (Float) fieldValue;
        else if (fieldValue instanceof Number)
          return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  protected double getHeuristicCost(final Vertex node, Vertex parent, final Vertex target, final CommandContext ctx) {
    double hresult = 0.0;

    // Ahead of every axis test on purpose: a custom formula owns h(n) outright, so it applies whether or not the
    // caller also declared vertex axes, and no tie-breaker is layered on top of its answer.
    if (paramHeuristicFormula == SQLHeuristicFormula.CUSTOM)
      return getCustomHeuristicCost(node, parent, target, currentDepth, ctx);

    if (paramVertexAxisNames.length == 0) {
      return hresult;
    } else if (paramVertexAxisNames.length == 1) {
      final double n = doubleOrDefault(node.get(paramVertexAxisNames[0]), 0.0);
      final double g = doubleOrDefault(target.get(paramVertexAxisNames[0]), 0.0);
      hresult = getSimpleHeuristicCost(n, g, paramDFactor);
    } else if (paramVertexAxisNames.length == 2) {
      if (parent == null)
        parent = node;
      final double sx = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[0]), 0);
      final double sy = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[1]), 0);
      final double nx = doubleOrDefault(node.get(paramVertexAxisNames[0]), 0);
      final double ny = doubleOrDefault(node.get(paramVertexAxisNames[1]), 0);
      final double px = doubleOrDefault(parent.get(paramVertexAxisNames[0]), 0);
      final double py = doubleOrDefault(parent.get(paramVertexAxisNames[1]), 0);
      final double gx = doubleOrDefault(target.get(paramVertexAxisNames[0]), 0);
      final double gy = doubleOrDefault(target.get(paramVertexAxisNames[1]), 0);

      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
      }

    } else {
      final Map<String, Double> sList = new HashMap<String, Double>();
      final Map<String, Double> cList = new HashMap<String, Double>();
      final Map<String, Double> pList = new HashMap<String, Double>();
      final Map<String, Double> gList = new HashMap<String, Double>();
      parent = parent == null ? node : parent;
      for (int i = 0; i < paramVertexAxisNames.length; i++) {
        final Double s = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[i]), 0);
        final Double c = doubleOrDefault(node.get(paramVertexAxisNames[i]), 0);
        final Double g = doubleOrDefault(target.get(paramVertexAxisNames[i]), 0);
        final Double p = doubleOrDefault(parent.get(paramVertexAxisNames[i]), 0);
        if (s != null)
          sList.put(paramVertexAxisNames[i], s);
        if (c != null)
          // THE CURRENT NODE'S OWN COORDINATE, NOT THE SOURCE'S: cList IS THE POSITION EVERY HEURISTIC BELOW MEASURES
          // FROM, AND FILLING IT WITH s MADE h(n) CONSTANT OVER THE WHOLE SEARCH (ISSUE #6385).
          cList.put(paramVertexAxisNames[i], c);
        if (g != null)
          gList.put(paramVertexAxisNames[i], g);
        if (p != null)
          pList.put(paramVertexAxisNames[i], p);
      }
      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, hresult);
      }

    }

    return hresult;

  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }

}
