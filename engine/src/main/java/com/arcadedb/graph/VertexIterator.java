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

import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexIterator implements Iterator<Vertex>, Iterable<Vertex> {
  private       EdgeSegment   currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  public VertexIterator(final EdgeSegment current) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");
    this.currentContainer = current;
  }

  @Override
  public boolean hasNext() {
    if (currentContainer == null)
      return false;

    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
      return currentPosition.get() < currentContainer.getUsed();
    }
    return false;
  }

  @Override
  public Vertex next() {
    while (true) {
      if (!hasNext())
        throw new NoSuchElementException();

      currentContainer.getRID(currentPosition); // SKIP EDGE
      final RID rid = currentContainer.getRID(currentPosition);

      try {
        return rid.asVertex();
      } catch (RecordNotFoundException e) {
        // SKIP
      }
    }
  }

  @Override
  public Iterator<Vertex> iterator() {
    return this;
  }
}
