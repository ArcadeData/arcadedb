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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.utility.Pair;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by luigidellaquila on 02/08/16.
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  private final boolean asc;
  private final Set<String> seenKeyRIDPairs = new HashSet<>();

  public FetchFromIndexValuesStep(final RangeIndex index, final boolean asc, final CommandContext context) {
    super(index, null, null, context);
    this.asc = asc;
  }

  @Override
  protected boolean isOrderAsc() {
    return asc;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // Override to add deduplication for ORDER BY index optimization
    init(context.getDatabase());
    pullPrevious(context, nRecords);

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords)
          return false;

        // Keep fetching until we find a non-duplicate entry or run out of entries
        while (nextEntry == null || (nextEntry.getSecond() != null && isKeyRIDPairSeen(nextEntry))) {
          if (nextEntry != null && nextEntry.getSecond() != null) {
            // Skip this duplicate and fetch the next one
            nextEntry = null;
          }
          fetchNextEntry();
          if (nextEntry == null) {
            break;
          }
        }

        return nextEntry != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final Object key = nextEntry.getFirst();
          final Identifiable value = nextEntry.getSecond();
          
          // Add the key-RID pair to seen set (hasNext() already checked for duplicates)
          if (value != null) {
            addKeyRIDPair(nextEntry);
          }

          nextEntry = null;

          localCount++;
          final ResultInternal result = new ResultInternal();
          result.setProperty("key", key);
          result.setProperty("rid", value);
          context.setVariable("current", result);
          return result;
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    if (isOrderAsc()) {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VALUES ASC " + index.getName();
    } else {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VALUES DESC " + index.getName();
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  private boolean isKeyRIDPairSeen(final Pair<Object, Identifiable> entry) {
    final String keyRIDPair = createKeyRIDPairString(entry);
    return seenKeyRIDPairs.contains(keyRIDPair);
  }

  private void addKeyRIDPair(final Pair<Object, Identifiable> entry) {
    final String keyRIDPair = createKeyRIDPairString(entry);
    seenKeyRIDPairs.add(keyRIDPair);
  }

  private String createKeyRIDPairString(final Pair<Object, Identifiable> entry) {
    Object key = entry.getFirst();
    Object rid = entry.getSecond() != null ? entry.getSecond().getIdentity() : null;
    return key + ":" + rid;
  }
}
