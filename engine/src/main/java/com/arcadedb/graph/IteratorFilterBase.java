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
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;

import java.util.*;
import java.util.logging.*;

public abstract class IteratorFilterBase<T> extends ResettableIteratorBase<T> {
  private         int          lastElementPosition   = currentPosition.get();
  protected       RID          nextEdge;
  protected       RID          nextVertex;
  protected       RID          next;
  protected final Set<Integer> validBuckets;
  protected       int          fullStackTracePrinted = 0;

  protected IteratorFilterBase(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current);

    validBuckets = new HashSet<>();
    for (final String e : edgeTypes) {
      if (!database.getSchema().existsType(e))
        continue;

      final EdgeType type = (EdgeType) database.getSchema().getType(e);

      validBuckets.addAll(type.getBucketIds(true));
    }
  }

  protected boolean hasNext(final boolean edge) {
    if (next != null)
      return true;

    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed()) {
        lastElementPosition = currentPosition.get();

        nextEdge = currentContainer.getRID(currentPosition);
        nextVertex = currentContainer.getRID(currentPosition);

        if (!validBuckets.contains(nextEdge.getBucketId())) {
          // FILTER IT OUT
          nextEdge = null;
          nextVertex = null;
          next = null;
          continue;
        }

        if (edge) {
          next = nextEdge;

          // VALIDATE RID
          if (nextEdge.getPosition() > -1)
            try {
              database.lookupByRID(nextEdge, false);
            } catch (final Exception e) {
              handleCorruption(e, nextEdge, nextVertex);
              continue;
            }

        } else {
          next = nextVertex;

          // VALIDATE RID
          try {
            database.lookupByRID(nextVertex, false);
          } catch (final Exception e) {
            handleCorruption(e, nextEdge, nextVertex);
            continue;
          }
        }

        return true;

      } else {
        // FETCH NEXT CHUNK
        currentContainer = currentContainer.getPrevious();
        if (currentContainer != null) {
          currentPosition.set(currentContainer.getContentStartOffset());
          lastElementPosition = currentPosition.get();
        } else
          // END
          break;
      }
    }

    return false;
  }

  protected void handleCorruption(final Exception e, final RID edge, final RID nextVertex) {
    if (fullStackTracePrinted < 10) {
      ++fullStackTracePrinted;
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s. Skip it.", e, edge);
    } else
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s. Skip it. Error: %s", edge, e.getMessage());
  }

  @Override
  public void remove() {
    if (currentContainer != null) {
      currentContainer.removeEntry(lastElementPosition, currentPosition.get());
      database.updateRecord(currentContainer);

      // v1 format: If removing from a continuation segment, also update the head segment's cached count
      if (currentContainer != initialContainer && initialContainer instanceof MutableEdgeSegment) {
        ((MutableEdgeSegment) initialContainer).decrementTotalCount();
        database.updateRecord(initialContainer);
      }

      currentPosition.set(lastElementPosition);
    }
  }

  public RID getNextVertex() {
    return nextVertex;
  }
}
