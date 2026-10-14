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

import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Merges the branches of an OR whose every branch answers best first, by descending {@code $score} and equal scores by
 * ascending RID, into one stream in that same order (issue #10022). It takes the place of the {@link ParallelExecStep} that
 * runs the branches one after the other, the {@link DistinctExecutionStep} that deduplicates them and the sort of an
 * {@code ORDER BY $score DESC}: every branch keeps its lazy loading, so a LIMIT loads about the rows it returns instead of
 * every match of every branch.
 * <p>
 * A branch sets {@code $score} for each row it produces, as its filter evaluates the scoring condition again, and that
 * variable is the only place the score lives. The step therefore reads it right after it pulls a row from a branch, before
 * it pulls from another, and sets it back when it hands the row on, which is when the steps after it read it. That relies
 * on a branch evaluating its rows one at a time, whatever batch size it is asked for: its last step is a
 * {@link FilterStep}, whose {@code next()} returns the row its condition was just evaluated on and evaluates the following
 * one only on the next {@code hasNext()}. A step that evaluated rows ahead of the one it returns would break the merge. A record
 * matching several branches is served once, at the first of its positions in the merged order, which is its best score.
 * <p>
 * The planner chains this step only when every branch is such a fetch and nothing in a branch's filter can set
 * {@code $score} to another value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MergeByScoreStep extends AbstractExecutionStep {
  private static final String SCORE_VARIABLE = "$score";

  private final List<InternalExecutionPlan> subExecutionPlans;
  // Per branch: the rows of its current batch, its next row with the score it had, and whether it has no row left
  private final ResultSet[]                 batches;
  private final Result[]                    heads;
  private final float[]                     headScores;
  private final boolean[]                   exhausted;

  // The records already served, under the heap budget of all the queries as DISTINCT's are
  private final RidSet             emitted;
  private final OperationHeapLimit heapLimit;
  private       WorkGuard          guard;
  private       int                consumed;

  private Result nextValue;
  private float  nextScore;

  public MergeByScoreStep(final List<InternalExecutionPlan> subExecutionPlans, final CommandContext context) {
    super(context);
    // The scores are read off the context right after each row: that needs every branch to end with the filter that
    // evaluates its scoring condition one row at a time (see the class Javadoc). Refused here rather than misordered later
    for (final InternalExecutionPlan plan : subExecutionPlans)
      if (!(plan instanceof final SelectExecutionPlan select) || select.getSteps().isEmpty()
          || !(select.getSteps().getLast() instanceof FilterStep))
        throw new IllegalArgumentException("Every branch merged by $score must end with the filter that scores its rows");
    this.subExecutionPlans = subExecutionPlans;
    final int branches = subExecutionPlans.size();
    this.batches = new ResultSet[branches];
    this.heads = new Result[branches];
    this.headScores = new float[branches];
    this.exhausted = new boolean[branches];
    this.emitted = new RidSet(context);
    this.heapLimit = OperationHeapLimit.of(context, "OR MERGE BY $score");
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords)
          return false;
        if (nextValue == null)
          fetchNext(nRecords);
        return nextValue != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextValue;
        final float score = nextScore;
        nextValue = null;
        localCount++;
        // Pulling the other branches overwrote it: the steps after this one read the score of the row they receive
        context.setVariable(SCORE_VARIABLE, score);
        return result;
      }
    };
  }

  private void fetchNext(final int nRecords) {
    if (guard == null)
      guard = WorkGuard.forCommandDeadline(context);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      Result candidate;
      float score;
      do {
        // Skipping duplicates consumes input without producing a row, as long as the duplicates last (issue #9680)
        guard.checkPeriodically(++consumed);
        int best = -1;
        for (int i = 0; i < heads.length; i++) {
          if (heads[i] == null && !exhausted[i])
            pullHead(i, nRecords);
          if (heads[i] != null && (best < 0 || precedes(i, best)))
            best = i;
        }
        if (best < 0) {
          // Every branch is drained: no RID needs remembering anymore, even if the consumer keeps the result set open
          releaseBuffer();
          return;
        }
        candidate = heads[best];
        score = headScores[best];
        heads[best] = null;
      } while (alreadyEmitted(candidate));
      nextValue = candidate;
      nextScore = score;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** The next row of a branch and its score, read before any other branch can overwrite it. */
  private void pullHead(final int branch, final int nRecords) {
    ResultSet batch = batches[branch];
    if (batch == null || !batch.hasNext()) {
      batch = subExecutionPlans.get(branch).fetchNext(nRecords);
      batches[branch] = batch;
      if (!batch.hasNext()) {
        exhausted[branch] = true;
        batches[branch] = null;
        return;
      }
    }
    heads[branch] = batch.next();
    final Object score = context.getVariable(SCORE_VARIABLE);
    headScores[branch] = score instanceof Number number ? number.floatValue() : 0f;
  }

  /** Whether the head of branch {@code a} comes before the head of branch {@code b}: higher score, then lower RID. */
  private boolean precedes(final int a, final int b) {
    final int byScore = Float.compare(headScores[b], headScores[a]);
    if (byScore != 0)
      return byScore < 0;
    final RID ridA = identityOf(heads[a]);
    final RID ridB = identityOf(heads[b]);
    // A row with no RID - none of the fetches the planner merges produces one - goes after the records of its score
    if (ridA == null || ridB == null)
      return ridA != null;
    return ridA.compareTo(ridB) < 0;
  }

  private boolean alreadyEmitted(final Result row) {
    final RID rid = identityOf(row);
    if (rid == null || rid.getBucketId() < 0 || rid.getPosition() < 0)
      // not a stored record: nothing to deduplicate it by, and no fetch of an indexed function produces one
      return false;
    final long bitmapBytes = emitted.getAllocatedBytes();
    if (!emitted.add(rid))
      return true;
    final long grown = emitted.getAllocatedBytes() - bitmapBytes;
    if (grown > 0) {
      try {
        heapLimit.charge(grown);
      } catch (final RuntimeException e) {
        releaseBuffer();
        throw e;
      }
    }
    return false;
  }

  private static RID identityOf(final Result row) {
    return row.isElement() ? row.getElement().get().getIdentity() : null;
  }

  private void releaseBuffer() {
    emitted.clear();
    heapLimit.release();
  }

  @Override
  public void sendTimeout() {
    // DO NOT PROPAGATE TIMEOUT
  }

  @Override
  public void reset() {
    releaseBuffer();
    for (int i = 0; i < heads.length; i++) {
      batches[i] = null;
      heads[i] = null;
      exhausted[i] = false;
    }
    nextValue = null;
    consumed = 0;
    guard = null;
    for (final InternalExecutionPlan plan : subExecutionPlans)
      plan.reset(context);
  }

  @Override
  public void close() {
    releaseBuffer();
    // The branches may hold open index cursors when the consumer stopped before draining them (a LIMIT)
    for (final InternalExecutionPlan plan : subExecutionPlans)
      plan.close();
    super.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder(ind).append("+ MERGE BY $score DESC");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    for (int i = 0; i < subExecutionPlans.size(); i++) {
      result.append("\n").append(ind).append("  +-- BRANCH ").append(i + 1);
      result.append("\n").append(subExecutionPlans.get(i).prettyPrint(depth + 2, indent));
    }
    return result.toString();
  }

  public List<ExecutionPlan> getSubExecutionPlans() {
    return new ArrayList<>(subExecutionPlans);
  }

  @Override
  public boolean canBeCached() {
    for (final InternalExecutionPlan plan : subExecutionPlans)
      if (!plan.canBeCached())
        return false;
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    final List<InternalExecutionPlan> copies = new ArrayList<>(subExecutionPlans.size());
    for (final InternalExecutionPlan plan : subExecutionPlans)
      copies.add(plan.copy(context));
    return new MergeByScoreStep(copies, context);
  }
}
