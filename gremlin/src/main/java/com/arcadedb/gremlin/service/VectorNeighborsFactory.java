/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.gremlin.service;

import com.arcadedb.database.Identifiable;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.utility.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.*;

/**
 * Uses the vector index HNSW to retrieve the top K neighbors from a key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorNeighborsFactory extends ArcadeServiceRegistry.ArcadeServiceFactory<Vertex, List<Map>> implements Service<Vertex, List<Map>> {

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
    return Collections.singleton(Type.Start);
  }

  @Override
  public Service<Vertex, List<Map>> createService(final boolean isStart, final Map params) {
    return this;
  }

  public CloseableIterator<List<Map>> execute(final ServiceCallContext ctx, final Map params) {
    final String indexName = (String) params.get("indexName");
    final Object vector = params.get("vector");
    Integer limit = (Integer) params.get("limit");
    if (limit == null)
      limit = -1;

    final HnswVectorIndex persistentIndex = (HnswVectorIndex) graph.getDatabase().getSchema().getIndexByName(indexName);
    final List<Pair<Identifiable, ? extends Number>> neighbors = persistentIndex.findNeighborsFromVector(vector, limit);

    final List<Map> result = new ArrayList<>(neighbors.size());
    for (Pair<Identifiable, ? extends Number> n : neighbors)
      result.add(Map.of("vertex", graph.getVertexFromRecord(n.getFirst()), "distance", n.getSecond()));
    return CloseableIterator.of(Collections.singletonList(result).iterator());
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
