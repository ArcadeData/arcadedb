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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.schema.EdgeType;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator that returns connected vertex RIDs from edge segments, filtered by edge type,
 * without loading vertex records from disk. Unlike {@link VertexIteratorFilter}, this iterator
 * skips the {@code lookupByRID} validation call, making it significantly faster for bulk
 * neighbor enumeration where only RIDs are needed.
 */
public class RIDIteratorFilter extends ResettableIteratorBase<RID> {
  private final Set<Integer> validBuckets;
  private RID next;

  public RIDIteratorFilter(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current);

    validBuckets = new HashSet<>();
    for (final String e : edgeTypes) {
      if (!database.getSchema().existsType(e))
        continue;
      final EdgeType type = (EdgeType) database.getSchema().getType(e);
      validBuckets.addAll(type.getBucketIds(true));
    }
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;

    if (currentContainer == null || validBuckets.isEmpty())
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed()) {
        final RID edgeRID = currentContainer.getRID(currentPosition);
        final RID vertexRID = currentContainer.getRID(currentPosition);

        if (!validBuckets.contains(edgeRID.getBucketId()))
          continue;

        next = vertexRID;
        return true;
      } else {
        currentContainer = currentContainer.getPrevious();
        if (currentContainer != null)
          currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
        else
          break;
      }
    }

    return false;
  }

  @Override
  public RID next() {
    if (!hasNext())
      throw new NoSuchElementException();

    try {
      return next;
    } finally {
      next = null;
      ++browsed;
    }
  }
}
