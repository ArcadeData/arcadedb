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

import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Base of every {@code schema:} catalog step that answers a LISTING - types, buckets, indexes, triggers,
 * materialized views, continuous aggregates, graph analytical views.
 * <p>
 * Each of them used to materialise its whole listing on the first pull and then hand back a {@code ResultSet} over
 * the entire thing, ignoring the {@code nRecords} the caller asked for. Both paging steps above them assume a
 * batch is bounded by what they requested, so a single over-long batch broke them in opposite directions
 * (issue #7898): {@code LIMIT 2} over six types answered six rows, and {@code SKIP 1} answered none at all,
 * because {@link SkipExecutionStep} discards the batch it receives rather than the rows it still owes. A listing
 * step of exactly this shape was added for {@code schema:triggers} the same week the counting half of the problem
 * was fixed, which is what made a shared base worth having: the rule now lives in one place instead of being
 * re-derived, and re-broken, per catalog.
 * <p>
 * The listing is still materialised in one pass - a catalog is small, it is read off live schema objects, and
 * paging the source would mean holding schema state across pulls - but it is HANDED OUT {@code nRecords} at a
 * time, with {@link #cursor} left where the batch stopped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractFetchFromSchemaListStep extends AbstractExecutionStep {

  /** The whole listing, materialised on the first pull. */
  protected final List<ResultInternal> result = new ArrayList<>();

  /** How far {@link #result} has been handed out. */
  private int cursor = 0;

  /**
   * Whether {@link #fetchListing} has run. A separate flag and not {@code cursor == 0}: a listing that legitimately
   * answers zero rows leaves the cursor at zero forever, which would re-read the catalog on every pull, and a
   * {@code reset()} would append a second copy of every row to {@link #result}.
   */
  private boolean materialized = false;

  protected AbstractFetchFromSchemaListStep(final CommandContext context) {
    super(context);
  }

  /**
   * Fills {@link #result} with the whole listing. Called once, on the first pull, already inside the profiling
   * window.
   */
  protected abstract void fetchListing(CommandContext context);

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (!materialized) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        fetchListing(context);
      } catch (final RuntimeException e) {
        // A listing that died half-way must not be served, and must not be appended to by a retry either: the
        // flag stays false, so the rows already collected have to go.
        result.clear();
        throw e;
      } finally {
        if (context.isProfiling())
          cost += System.nanoTime() - begin;
      }
      materialized = true;
    }

    // The bounds of THIS batch, fixed when the batch is created: nRecords is what the caller asked for, and a
    // caller that keeps pulling gets the next slice. A non-positive nRecords means "no bound" - the same reading
    // every other source step in this package gives it.
    final int batchStart = cursor;
    final int batchEnd = nRecords > 0 ? Math.min(result.size(), cursor + nRecords) : result.size();

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < batchEnd;
      }

      @Override
      public Result next() {
        if (cursor >= batchEnd)
          throw new NoSuchElementException();
        return result.get(cursor++);
      }

      @Override
      public void close() {
        // Nothing to release: the whole listing is already in memory by the time this result set exists.
      }

      /**
       * Back to the start of THIS batch, not of the whole listing. {@code cursor} is one field shared by every
       * batch the step hands out, so rewinding it to zero would roll an older batch's reset under a newer
       * batch's feet - speculative today, since the pipeline consumes a batch before pulling the next, but the
       * per-batch answer is the faithful one anyway: an {@code InternalResultSet} resets to the start of its own
       * contents, and a batch's contents are its own slice (PR #8093 review).
       */
      @Override
      public void reset() {
        cursor = batchStart;
      }
    };
  }
}
