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
package com.arcadedb.gremlin.service;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.utility.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Uses the vector index to retrieve the top K neighbors from a key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorNeighborsFactory extends ArcadeServiceRegistry.ArcadeServiceFactory<Vertex, List<Map>>
    implements Service<Vertex, List<Map>> {

  public static final String NAME = "arcadedb#vectorNeighbors";

  private static final Map<String, String> PARAMS = Map.of(//
      "indexName", "Name of the index to use",//
      "vector", "vector where to find neighbors",//
      "limit", "max number of neighbors to return");

  public VectorNeighborsFactory(final ArcadeGraph graph) {
    super(graph, NAME);
  }

  @Override
  public Type getType() {
    return Type.Start;
  }

  @Override
  public Map describeParams() {
    return PARAMS;
  }

  @Override
  public Set<Type> getSupportedTypes() {
    return Set.of(Type.Start);
  }

  @Override
  public Service<Vertex, List<Map>> createService(final boolean isStart, final Map params) {
    return this;
  }

  public CloseableIterator<List<Map>> execute(final ServiceCallContext ctx, final Map params) {
    final String indexName = (String) params.get("indexName");
    final Object vectorParam = params.get("vector");

    Integer limit = (Integer) params.get("limit");
    if (limit == null)
      limit = -1;
    if (vectorParam instanceof float[] vector) {

      TypeIndex indexByName = (TypeIndex) graph.getDatabase().getSchema().getIndexByName(indexName);
      final LSMVectorIndex persistentIndex = (LSMVectorIndex) indexByName.getIndexesOnBuckets()[0];
      final List<Pair<RID, Float>> neighbors = persistentIndex.findNeighborsFromVector(vector, limit);

      final List<Map> result = new ArrayList<>(neighbors.size());
      for (Pair<RID, Float> n : neighbors)
        result.add(Map.of("vertex", graph.getVertexFromRecord(n.getFirst()), "distance", n.getSecond()));
      return CloseableIterator.of(List.of(result).iterator());
    }
    else return CloseableIterator.empty();
  }

  @Override
  public CloseableIterator<List<Map>> execute(final ServiceCallContext ctx, final Traverser.Admin<Vertex> in, final Map params) {
    return execute(ctx, params);
  }

  @Override
  public CloseableIterator<List<Map>> execute(final ServiceCallContext ctx, final TraverserSet<Vertex> in, final Map params) {
    return execute(ctx, params);
  }

  @Override
  public void close() {
  }
}
