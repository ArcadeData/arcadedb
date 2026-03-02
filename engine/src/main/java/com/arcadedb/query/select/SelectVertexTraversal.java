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
package com.arcadedb.query.select;

import com.arcadedb.graph.Vertex;

import java.util.*;
import java.util.stream.*;

/**
 * Iterator that lazily expands source vertices to adjacent vertices in a given direction.
 * Supports chaining to further vertex or edge traversals.
 */
public class SelectVertexTraversal implements Iterator<Vertex> {
  private final Iterator<Vertex> source;
  private final Vertex.DIRECTION direction;
  private final String[]         edgeTypes;
  private       Iterator<Vertex> currentNeighbors;

  public SelectVertexTraversal(final Iterator<Vertex> source, final Vertex.DIRECTION direction, final String... edgeTypes) {
    this.source = source;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  @Override
  public boolean hasNext() {
    while (currentNeighbors == null || !currentNeighbors.hasNext()) {
      if (!source.hasNext())
        return false;
      currentNeighbors = source.next().getVertices(direction, edgeTypes).iterator();
    }
    return true;
  }

  @Override
  public Vertex next() {
    if (!hasNext())
      throw new NoSuchElementException();
    return currentNeighbors.next();
  }

  public SelectVertexTraversal thenOutVertices(final String... edgeTypes) {
    return new SelectVertexTraversal(this, Vertex.DIRECTION.OUT, edgeTypes);
  }

  public SelectVertexTraversal thenInVertices(final String... edgeTypes) {
    return new SelectVertexTraversal(this, Vertex.DIRECTION.IN, edgeTypes);
  }

  public SelectVertexTraversal thenBothVertices(final String... edgeTypes) {
    return new SelectVertexTraversal(this, Vertex.DIRECTION.BOTH, edgeTypes);
  }

  public SelectEdgeTraversal thenOutEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal(this, Vertex.DIRECTION.OUT, edgeTypes);
  }

  public SelectEdgeTraversal thenInEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal(this, Vertex.DIRECTION.IN, edgeTypes);
  }

  public SelectEdgeTraversal thenBothEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal(this, Vertex.DIRECTION.BOTH, edgeTypes);
  }

  public List<Vertex> toList() {
    final List<Vertex> result = new ArrayList<>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public Stream<Vertex> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL), false);
  }
}
