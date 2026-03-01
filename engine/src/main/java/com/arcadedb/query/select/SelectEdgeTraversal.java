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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.*;
import java.util.stream.*;

/**
 * Iterator that lazily expands source vertices to adjacent edges in a given direction.
 */
public class SelectEdgeTraversal implements Iterator<Edge> {
  private final Iterator<Vertex> source;
  private final Vertex.DIRECTION direction;
  private final String[]         edgeTypes;
  private       Iterator<Edge>   currentEdges;

  public SelectEdgeTraversal(final Iterator<Vertex> source, final Vertex.DIRECTION direction, final String... edgeTypes) {
    this.source = source;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  @Override
  public boolean hasNext() {
    while (currentEdges == null || !currentEdges.hasNext()) {
      if (!source.hasNext())
        return false;
      currentEdges = source.next().getEdges(direction, edgeTypes).iterator();
    }
    return true;
  }

  @Override
  public Edge next() {
    if (!hasNext())
      throw new NoSuchElementException();
    return currentEdges.next();
  }

  public List<Edge> toList() {
    final List<Edge> result = new ArrayList<>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public Stream<Edge> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL), false);
  }
}
