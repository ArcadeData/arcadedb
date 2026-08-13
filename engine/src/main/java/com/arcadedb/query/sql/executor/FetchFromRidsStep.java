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
import com.arcadedb.query.sql.parser.BooleanExpression;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private       Iterable<RID>     rids;
  // Set only by the cache-friendly constructor used by SelectExecutionPlanner.handleTypeWithRidFilter
  // (WHERE @rid = ? / @rid IN ?, see issue #5855). When non-null, `rids` above is re-resolved lazily
  // against the live CommandContext on the first pull of every execution instead of being baked in
  // at plan-build time, mirroring how FetchFromTypeWithFilterStep re-evaluates its WhereClause. This
  // is what makes the step - and therefore the whole plan - safely reusable from the execution plan
  // cache across executions bound to different RIDs.
  private final BooleanExpression ridCondition;
  private       Iterator<RID>     iterator;
  private       Result            nextResult = null;

  public FetchFromRidsStep(final Iterable<RID> rids, final CommandContext context) {
    super(context);
    this.rids = rids;
    this.ridCondition = null;
    reset();
  }

  FetchFromRidsStep(final BooleanExpression ridCondition, final CommandContext context) {
    super(context);
    this.ridCondition = ridCondition;
  }

  public void reset() {
    // With a ridCondition, defer re-resolution to the first pull of the (re)started execution, so it
    // uses that execution's own live CommandContext/bound parameters rather than whichever context
    // reset() happens to be called without.
    iterator = ridCondition == null ? rids.iterator() : null;
    nextResult = null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    if (ridCondition != null && iterator == null) {
      this.rids = SelectExecutionPlanner.resolveRidEqualityOrInListAtRuntime(ridCondition, context);
      iterator = this.rids.iterator();
    }
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

          final Identifiable nextDoc;
          try {
            // Eagerly verify the record exists before wrapping it. Under READ_COMMITTED isolation,
            // lookupByRID(rid, false) returns a lazy stub without touching the bucket, so a missing
            // RID would only surface as RecordNotFoundException later during materialization (escaping
            // as "Error on transaction commit"). The existence probe keeps this step isolation
            // independent: a RID that cannot be resolved is simply skipped. See issue #4643.
            if (!context.getDatabase().existsRecord(nextRid))
              continue;
            nextDoc = context.getDatabase().lookupByRID(nextRid, false);
          } catch (final RecordNotFoundException e) {
            continue;
          }

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
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM RIDs\n" + ExecutionStepInternal.getIndent(depth, indent) + "  "
        + (ridCondition != null ? ridCondition : rids);
  }

  @Override
  public boolean canBeCached() {
    return ridCondition != null;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    if (ridCondition == null)
      throw new UnsupportedOperationException();
    return new FetchFromRidsStep(ridCondition, context);
  }
}
