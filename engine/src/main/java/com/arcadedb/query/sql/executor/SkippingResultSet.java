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

import java.util.NoSuchElementException;

/**
 * One pull of a step that acts on each upstream row and passes on only some of them: {@link DeleteStep} and
 * {@link SaveElementStep}, which drop the rows whose write a before-event listener vetoed (issue #9974).
 * <p>
 * <b>The rule that makes dropping safe.</b> The consumers of a pull ({@link CountStep}, {@code UpdateExecutionPlan}) stop at the
 * first pull that comes back EMPTY. So a pull must never answer empty while upstream still has rows: when every row of the batch
 * was dropped, the next batch is pulled here, as {@link FilterStep} does. Once this pull has passed rows on, the end of the batch
 * ends it: its consumer pulls again and the next pull starts on the next batch, so nothing is skipped and the pull stays bounded by
 * one upstream batch. {@code finished} is sticky only for this one pull. When EVERY row upstream is dropped, one pull reads the
 * whole upstream before answering empty, exactly as a {@link FilterStep} that matches nothing does.
 * <p>
 * The row is acted on in {@link #hasNext()}, one row ahead of {@link #next()}: a consumer that asks whether there is a next row
 * learns it only once the write has been applied or refused, and a write failure is raised from {@code hasNext()}. That is never
 * a row written and not returned: the plans these steps run in ({@code UpdateExecutionPlan}, {@code InsertExecutionPlan}) drain
 * every pull before the caller sees a row, and a {@code LIMIT} is applied BEFORE the write step, never after it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class SkippingResultSet implements ResultSet {
  private final ExecutionStepInternal prevStep;
  private final CommandContext        context;
  private final int                   nRecords;
  private       ResultSet             upstream;
  private       Result                nextItem;
  private       boolean               finished;
  private       int                   fetched;

  SkippingResultSet(final ExecutionStepInternal prevStep, final CommandContext context, final int nRecords) {
    this.prevStep = prevStep;
    this.context = context;
    this.nRecords = nRecords;
    this.upstream = prevStep.syncPull(context, nRecords);
  }

  /**
   * Acts on one upstream row.
   *
   * @return the row to pass on, {@code null} to drop it
   */
  protected abstract Result process(Result row);

  private void fetchNextItem() {
    while (nextItem == null && !finished) {
      while (!upstream.hasNext()) {
        if (fetched > 0) {
          // this pull already passed rows on: the end of its batch ends it, the consumer pulls again
          finished = true;
          return;
        }
        // the drained batch is NOT closed: a step's ResultSet may close the step itself on close() (FilterStep's does), which
        // would end the very pull that comes next. The step chain is closed once, by the plan
        upstream = prevStep.syncPull(context, nRecords);
        if (!upstream.hasNext()) {
          finished = true;
          return;
        }
      }
      nextItem = process(upstream.next());
    }
  }

  @Override
  public boolean hasNext() {
    if (nextItem == null)
      fetchNextItem();
    return nextItem != null;
  }

  @Override
  public Result next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final Result result = nextItem;
    nextItem = null;
    fetched++;
    return result;
  }

  @Override
  public void close() {
    upstream.close();
  }
}
