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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.*;

/**
 * Abstract base class for algorithm procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractAlgoProcedure implements CypherProcedure {

  // ── Embedding math utilities ─────────────────────────────────────────────

  /** Normalises {@code vec} to unit L2 length in-place; no-op if the vector is zero. */
  protected static void normalizeL2(final double[] vec) {
    double norm = 0.0;
    for (final double v : vec)
      norm += v * v;
    if (norm == 0.0)
      return;
    norm = Math.sqrt(norm);
    for (int i = 0; i < vec.length; i++)
      vec[i] /= norm;
  }

  /** Returns the dot product of two equal-length vectors. */
  protected static double dot(final double[] a, final double[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++)
      s += a[i] * b[i];
    return s;
  }

  /** Logistic sigmoid: σ(x) = 1 / (1 + e^{-x}), clamped to avoid overflow. */
  protected static double sigmoid(final double x) {
    return 1.0 / (1.0 + Math.exp(-x));
  }

  /** Converts a {@code double[]} to an unmodifiable {@code List<Double>} for Cypher return. */
  protected static List<Double> toEmbeddingList(final double[] vec) {
    final List<Double> list = new ArrayList<>(vec.length);
    for (final double v : vec)
      list.add(v);
    return list;
  }

  // ── Argument extractors ──────────────────────────────────────────────────

  /**
   * Extracts a list of vertices from an argument that may be a {@code List<Vertex>},
   * a single {@code Vertex}, or similar.
   */
  @SuppressWarnings("unchecked")
  protected List<Vertex> extractVertexList(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    if (arg instanceof List<?> list) {
      final List<Vertex> result = new ArrayList<>(list.size());
      for (final Object item : list)
        result.add(extractVertex(item, paramName + "[*]"));
      return result;
    }
    if (arg instanceof Vertex v)
      return List.of(v);
    throw new IllegalArgumentException(getName() + "(): " + paramName + " must be a list of nodes");
  }

  protected Vertex extractVertex(final Object arg, final String paramName) {
    return switch (arg) {
      case null -> throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
      case Vertex vertex -> vertex;
      case Document doc when doc instanceof Vertex v -> v;
      default -> throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
    };
  }

  protected String extractString(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    return switch (arg) {
      case null -> null;
      case String s -> new String[]{s};
      case Collection<?> coll -> coll.stream().map(Object::toString).toArray(String[]::new);
      default -> new String[]{arg.toString()};
    };
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    else if (arg instanceof Map)
      return (Map<String, Object>) arg;

    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
  }

  /** @see GraphEngine#getAllVertices(Database, String[]) */
  protected Iterator<Vertex> getAllVertices(final Database db, final String[] nodeLabels) {
    return GraphEngine.getAllVertices(db, nodeLabels);
  }

  /** @see GraphEngine#buildRidIndex(List) */
  protected Map<RID, Integer> buildRidIndex(final List<Vertex> vertices) {
    return GraphEngine.buildRidIndex(vertices);
  }

  /** @see GraphEngine#neighborRid(Edge, RID, Vertex.DIRECTION) */
  protected RID neighborRid(final Edge edge, final RID sourceRid, final Vertex.DIRECTION dir) {
    return GraphEngine.neighborRid(edge, sourceRid, dir);
  }

  /** @see GraphEngine#parseDirection(String) */
  protected Vertex.DIRECTION parseDirection(final String dir) {
    return GraphEngine.parseDirection(dir);
  }

  /** @see GraphEngine#buildAdjacencyList(List, Map, Vertex.DIRECTION, String[]) */
  protected int[][] buildAdjacencyList(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx,
      final Vertex.DIRECTION dir, final String[] relTypes) {
    return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes);
  }

  /**
   * Finds a {@link GraphTraversalProvider} that covers all vertex and edge types, suitable for
   * whole-graph algorithms. Returns null if no suitable provider is available.
   *
   * @param db       the database
   * @param relTypes edge types to filter by (null = all types)
   */
  protected GraphTraversalProvider findProvider(final Database db, final String[] relTypes) {
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, relTypes);
    if (provider != null && provider.coversVertexType(null))
      return provider;
    return null;
  }

  /**
   * Builds an adjacency list (int[][]) from a {@link GraphTraversalProvider}'s CSR structure.
   * Each {@code result[i]} contains the dense neighbor IDs for node {@code i} in the given direction.
   */
  protected int[][] buildAdjacencyFromProvider(final GraphTraversalProvider provider, final Vertex.DIRECTION dir,
      final String[] relTypes) {
    final int n = provider.getNodeCount();
    final int[][] adj = new int[n][];
    for (int i = 0; i < n; i++)
      adj[i] = provider.getNeighborIds(i, dir, relTypes);
    return adj;
  }

  /**
   * Loads the graph structure, using CSR-backed adjacency from a {@link GraphTraversalProvider}
   * when available, otherwise falling back to OLTP (vertex/edge iteration).
   * <p>
   * Algorithms replace their manual vertex loading + {@code buildAdjacencyList()} calls with:
   * <pre>
   *   final GraphData graph = loadGraph(db, null, relTypes);
   *   final int[][] adj = graph.adjacency(Vertex.DIRECTION.OUT);
   *   // ... algorithm using adj[i] ...
   *   result.setProperty("node", graph.getVertex(i));
   * </pre>
   *
   * @param db         the database
   * @param nodeLabels vertex type filter (null = all types)
   * @param relTypes   edge type filter (null = all types)
   */
  protected GraphData loadGraph(final Database db, final String[] nodeLabels, final String[] relTypes) {
    return loadGraph(db, nodeLabels, relTypes, null);
  }

  protected GraphData loadGraph(final Database db, final String[] nodeLabels, final String[] relTypes,
      final CommandContext context) {
    if (nodeLabels == null || nodeLabels.length == 0) {
      final GraphTraversalProvider provider = findProvider(db, relTypes);
      if (provider != null) {
        if (context != null)
          context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        return new GraphData(provider, provider.getNodeCount());
      }
    }
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, nodeLabels);
    while (iter.hasNext())
      vertices.add(iter.next());
    return new GraphData(vertices, buildRidIndex(vertices));
  }

  /**
   * Encapsulates graph data that can be backed by either a CSR provider or OLTP vertex lists.
   * Provides uniform access to adjacency, vertex lookup, and RID resolution regardless of backing.
   */
  protected static class GraphData {
    public final int                     nodeCount;
    private final GraphTraversalProvider provider;
    private final List<Vertex>           vertices;
    private final Map<RID, Integer>      ridToIdx;

    private GraphData(final GraphTraversalProvider provider, final int nodeCount) {
      this.provider = provider;
      this.vertices = null;
      this.ridToIdx = null;
      this.nodeCount = nodeCount;
    }

    private GraphData(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx) {
      this.provider = null;
      this.vertices = vertices;
      this.ridToIdx = ridToIdx;
      this.nodeCount = vertices.size();
    }

    public int[][] adjacency(final Vertex.DIRECTION dir, final String... relTypes) {
      if (provider != null) {
        final int[][] adj = new int[nodeCount][];
        for (int i = 0; i < nodeCount; i++)
          adj[i] = provider.getNeighborIds(i, dir, relTypes);
        return adj;
      }
      return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes);
    }

    public Vertex getVertex(final int i) {
      return provider != null ? provider.getRID(i).asVertex() : vertices.get(i);
    }

    public RID getRID(final int i) {
      return provider != null ? provider.getRID(i) : vertices.get(i).getIdentity();
    }

    public int indexOf(final RID rid) {
      if (provider != null)
        return provider.getNodeId(rid);
      final Integer idx = ridToIdx.get(rid);
      return idx != null ? idx : -1;
    }

    public boolean isCSRBacked() {
      return provider != null;
    }
  }

  /**
   * Builds a path representation from a list of RIDs.
   */
  protected Map<String, Object> buildPath(final List<RID> rids, final Database database) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final RID rid : rids) {
      final Document doc = database.lookupByRID(rid, true).asDocument();
      if (doc instanceof Vertex)
        nodes.add(doc);
      else if (doc instanceof Edge)
        relationships.add(doc);
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }
}
