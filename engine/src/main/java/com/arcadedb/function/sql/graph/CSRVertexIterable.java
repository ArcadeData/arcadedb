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

import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lazy iterable that converts CSR dense node IDs to Vertex objects on demand.
 * Used by SQL graph functions (out/in/both) when a GAV provider is available.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CSRVertexIterable implements Iterable<Vertex> {
  private final GraphTraversalProvider provider;
  private final int[]                  neighborIds;

  CSRVertexIterable(final GraphTraversalProvider provider, final int[] neighborIds) {
    this.provider = provider;
    this.neighborIds = neighborIds;
  }

  @Override
  public Iterator<Vertex> iterator() {
    return new Iterator<>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < neighborIds.length;
      }

      @Override
      public Vertex next() {
        if (index >= neighborIds.length)
          throw new NoSuchElementException();
        return provider.getRID(neighborIds[index++]).asVertex();
      }
    };
  }
}
