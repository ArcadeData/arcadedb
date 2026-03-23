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

import com.arcadedb.exception.RecordNotFoundException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lazy iterable that converts CSR dense node IDs to Vertex objects on demand.
 * Used by SQL graph functions (out/in/both) when a GAV provider is available.
 * <p>
 * Provides an O(1) {@link #size()} method that returns the neighbor count directly
 * from the CSR array length, avoiding the need to iterate all elements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRVertexIterable implements Iterable<Vertex> {
  private final GraphTraversalProvider provider;
  private final int[]                  neighborIds;

  public CSRVertexIterable(final GraphTraversalProvider provider, final int[] neighborIds) {
    this.provider = provider;
    this.neighborIds = neighborIds;
  }

  /**
   * Returns the neighbor count in O(1) directly from the CSR array length.
   * This avoids materializing all vertices just to count them.
   */
  public int size() {
    return neighborIds.length;
  }

  @Override
  public Iterator<Vertex> iterator() {
    return new Iterator<>() {
      private int index = 0;
      private Vertex nextVertex = null;

      @Override
      public boolean hasNext() {
        while (nextVertex == null && index < neighborIds.length) {
          final var rid = provider.getRID(neighborIds[index++]);
          if (rid != null) {
            try {
              nextVertex = rid.asVertex();
            } catch (final RecordNotFoundException e) {
              // vertex deleted in OLTP since CSR was built — skip
            }
          }
        }
        return nextVertex != null;
      }

      @Override
      public Vertex next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Vertex v = nextVertex;
        nextVertex = null;
        return v;
      }
    };
  }
}
