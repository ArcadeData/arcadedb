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
package com.arcadedb.graph;

import com.arcadedb.utility.ResettableIterator;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class EdgeToVertexIterable implements Iterable<Vertex> {
  private final Iterable<Edge>   edges;
  private final Vertex.DIRECTION direction;

  public EdgeToVertexIterable(final Iterable<Edge> edges, final Vertex.DIRECTION direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<Vertex> iterator() {
    final Iterator<Edge> iter = edges.iterator();
    if (iter instanceof ResettableIterator)
      return new EdgeToVertexIterator((ResettableIterator<Edge>) iter, direction);

    // The only non-ResettableIterator expected here is GraphEngine.EMPTY_EDGE_LIST's Collections.emptyIterator().
    // An empty iterator is safe to map to an empty result; a non-empty one would be silently dropped, so fail loudly.
    if (!iter.hasNext())
      return Collections.emptyIterator();
    throw new IllegalArgumentException("The edges iterator must be an instance of ResettableIterator when not empty");
  }
}
