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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;

import java.util.NoSuchElementException;

/**
 * Deletes records coming from upstream steps. Only the rows the engine actually deleted are passed on: a delete a
 * {@code BeforeRecordDeleteListener} vetoed is dropped here, so the {@link CountStep} after this step - the count SQL, the Postgres
 * wire and the HTTP API reply - counts deletions rather than candidates, and {@code RETURN BEFORE} returns only the records that are
 * gone (issue #9974). A row that is not a record (nothing to delete) is passed on as it always was.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class DeleteStep extends AbstractExecutionStep {

  public DeleteStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    final DatabaseInternal database = context.getDatabase();
    return new ResultSet() {
      private ResultSet upstream = prevStep.syncPull(context, nRecords);
      private Result    nextItem;
      private boolean   finished;
      private int       fetched;

      /**
       * A batch whose every row was vetoed must not end the stream: the consumers stop at the first empty pull, so the next batch
       * is pulled here instead, as {@link FilterStep} does.
       */
      private void fetchNextItem() {
        while (nextItem == null && !finished) {
          while (!upstream.hasNext()) {
            if (fetched > 0) {
              // this pull already returned rows: an empty batch here is a regular end of batch, the consumer pulls again
              finished = true;
              return;
            }
            upstream = prevStep.syncPull(context, nRecords);
            if (!upstream.hasNext()) {
              finished = true;
              return;
            }
          }

          final Result result = upstream.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (!result.isElement() || database.deleteRecordReportingVeto(result.getElement().get()))
              nextItem = result;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
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
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DELETE");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    return result.toString();
  }

}
