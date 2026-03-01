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
 * Iterator that expands source vertices to neighbors in a given direction, with optional edge type filter.
 * Supports chaining via {@code thenOut()}, {@code thenIn()}, {@code thenBoth()}.
 */
public class SelectTraversal implements Iterator<Vertex> {
  private final Iterator<Vertex>  source;
  private final Vertex.DIRECTION  direction;
  private final String[]          edgeTypes;
  private       Iterator<Vertex>  currentNeighbors;

  public SelectTraversal(final Iterator<Vertex> source, final Vertex.DIRECTION direction, final String... edgeTypes) {
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

  public SelectTraversal thenOut(final String... edgeTypes) {
    return new SelectTraversal(this, Vertex.DIRECTION.OUT, edgeTypes);
  }

  public SelectTraversal thenIn(final String... edgeTypes) {
    return new SelectTraversal(this, Vertex.DIRECTION.IN, edgeTypes);
  }

  public SelectTraversal thenBoth(final String... edgeTypes) {
    return new SelectTraversal(this, Vertex.DIRECTION.BOTH, edgeTypes);
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
