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
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private final Iterable<RID> rids;
  private       Iterator<RID> iterator;
  private       Result        nextResult = null;

  public FetchFromRidsStep(final Iterable<RID> rids, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.rids = rids;
    reset();
  }

  public void reset() {
    iterator = rids.iterator();
    nextResult = null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    return new ResultSet() {
      int internalNext = 0;

      private void fetchNext() {
        if (nextResult != null) {
          return;
        }
        while (iterator.hasNext()) {
          final RID nextRid = iterator.next();
          if (nextRid == null)
            continue;

          Identifiable nextDoc = null;
          try {
            nextDoc = context.getDatabase().lookupByRID(nextRid, false);
          } catch (final RecordNotFoundException e) {
            // IGNORE HERE< HANDLED BELOW
          }

          if (nextDoc == null)
            continue;

          nextResult = new ResultInternal(nextDoc);
          break;
        }
      }

      @Override
      public boolean hasNext() {
        if (internalNext >= nRecords)
          return false;

        if (nextResult == null)
          fetchNext();

        return nextResult != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        internalNext++;
        final Result result = nextResult;
        nextResult = null;
        return result;
      }

    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM RIDs\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + rids;
  }
}
