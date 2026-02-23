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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.conductance(communityProperty, relTypes?)
 * <p>
 * Computes the conductance of each community as identified by a vertex property.
 * Conductance(C) = cut(C, V\C) / min(vol(C), vol(V\C)) where cut = edges crossing
 * the community boundary, and vol(C) = sum of degrees of nodes in C.
 * Lower conductance indicates better community separation.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.conductance('communityId', 'KNOWS')
 * YIELD community, conductance, internalEdges, boundaryEdges, nodeCount
 * RETURN community, conductance ORDER BY conductance ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoConductance extends AbstractAlgoProcedure {
  public static final String NAME = "algo.conductance";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Computes conductance for each community identified by a vertex property";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("community", "conductance", "internalEdges", "boundaryEdges", "nodeCount");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String communityProperty = extractString(args[0], "communityProperty");
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // Read community IDs from vertex property
    final Object[] communityValues = new Object[n];
    // Map community value -> index
    final Map<Object, Integer> communityToIndex = new HashMap<>();
    int numCommunities = 0;

    for (int i = 0; i < n; i++) {
      final Object commVal = vertices.get(i).get(communityProperty);
      communityValues[i] = commVal;
      if (commVal != null && !communityToIndex.containsKey(commVal))
        communityToIndex.put(commVal, numCommunities++);
    }

    if (numCommunities == 0)
      return Stream.empty();

    // Map vertex index -> community index
    final int[] vertexCommunity = new int[n];
    for (int i = 0; i < n; i++) {
      final Object commVal = communityValues[i];
      vertexCommunity[i] = commVal == null ? -1 : communityToIndex.get(commVal);
    }

    // Accumulators per community
    final long[] internalEdges = new long[numCommunities];
    final long[] boundaryEdges = new long[numCommunities];
    final long[] totalDegree = new long[numCommunities];
    final long[] nodeCount = new long[numCommunities];

    for (int i = 0; i < n; i++) {
      final int ci = vertexCommunity[i];
      if (ci < 0)
        continue;
      nodeCount[ci]++;
      totalDegree[ci] += adj[i].length;
    }

    // Count edges: internal vs boundary
    // Use BOTH adjacency — each undirected edge is counted twice (once from each end)
    // To avoid double-counting for internal edges, we count directed edges and divide by 2
    // For boundary edges, each directed crossing counts once at each side, also divide by 2
    // Simpler: iterate all directed edges, classify as internal or boundary
    for (int u = 0; u < n; u++) {
      final int cu = vertexCommunity[u];
      if (cu < 0)
        continue;
      for (final int v : adj[u]) {
        final int cv = vertexCommunity[v];
        if (cv < 0)
          continue;
        if (cu == cv)
          internalEdges[cu]++;   // will be halved later
        else
          boundaryEdges[cu]++;   // edge from cu going out
      }
    }

    // Halve internal edges (each counted twice)
    for (int c = 0; c < numCommunities; c++)
      internalEdges[c] /= 2;

    // conductance(C) = cut(C, V\C) / min(vol(C), vol(V\C))
    // cut(C, V\C) = boundaryEdges[c] (each directed edge from C to outside counted once per direction — halve)
    // vol(V\C) = totalEdges - vol(C)  (we don't track this separately)
    // Compute total degree across all communities
    long totalAllDegree = 0;
    for (int c = 0; c < numCommunities; c++)
      totalAllDegree += totalDegree[c];

    // Build reverse map: community index -> original community value
    final Object[] indexToCommunity = new Object[numCommunities];
    for (final Map.Entry<Object, Integer> e : communityToIndex.entrySet())
      indexToCommunity[e.getValue()] = e.getKey();

    final List<Result> results = new ArrayList<>(numCommunities);
    for (int c = 0; c < numCommunities; c++) {
      // cut(C) = boundary edges from C (each undirected cut edge counted once from each side)
      final long cut = boundaryEdges[c] / 2;
      final long volC = totalDegree[c];
      final long volComplement = totalAllDegree - volC;
      final long minVol = Math.min(volC, volComplement);

      final double conductance = minVol == 0 ? 0.0 : (double) cut / minVol;

      final ResultInternal r = new ResultInternal();
      r.setProperty("community", indexToCommunity[c]);
      r.setProperty("conductance", conductance);
      r.setProperty("internalEdges", internalEdges[c]);
      r.setProperty("boundaryEdges", cut);
      r.setProperty("nodeCount", nodeCount[c]);
      results.add(r);
    }
    return results.stream();
  }
}
