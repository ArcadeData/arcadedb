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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.NoSuchElementException;

/**
 * <p>
 * takes a normal result set and transforms it in another result set made of OUpdatableRecord instances.
 * Records that are not identifiable are discarded.
 * </p>
 * <p>This is the opposite of ConvertToResultInternalStep</p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ConvertToUpdatableResultStep extends AbstractExecutionStep {
  static final String NOT_A_RECORD = "Cannot write to a row of the source that is not a record and carries no @rid (a projection "
      + "or a TIMESERIES sample). Select the records themselves (SELECT FROM <type> WHERE ...), or their @rid, as the source";
  ResultSet prevResult = null;

  public ConvertToUpdatableResultStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    return new ResultSet() {
      public boolean finished = false;

      Result nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (nextItem instanceof UpdatableResult) {
              break;
            }
            if (nextItem.isElement()) {
              final Record element = nextItem.getElement().get();
              if (element != null) {
                nextItem = new UpdatableResult(forUpdate(((Document) element.getRecord()).modify()));
              }
              break;
            }

            // Handle projected results containing a RID (e.g. from INSERT ... RETURN @rid)
            final Object ridValue = nextItem.getProperty("@rid");
            if (ridValue instanceof Identifiable identifiable) {
              final Record record = identifiable.getRecord();
              if (record instanceof Document doc) {
                nextItem = new UpdatableResult(forUpdate(doc.modify()));
                break;
              }
            }

            // A ROW THAT IS NOT A RECORD AND NAMES NONE (A PROJECTION, A TIMESERIES SAMPLE) HAS NOTHING TO WRITE TO: IT USED TO BE
            // SKIPPED, SO THE STATEMENT REPORTED count 0 AS IF NOTHING HAD MATCHED (issue #10336)
            throw new CommandExecutionException(NOT_A_RECORD);
          } finally {
            cost = System.nanoTime() - begin;
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        return nextItem != null;

      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished) {
          throw new NoSuchElementException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new NoSuchElementException();
        }
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        context.setVariable("current", result);
        return result;
      }

      @Override
      public void close() {
        ConvertToUpdatableResultStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CONVERT TO UPDATABLE ITEM";
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  /**
   * #8610: the UPDATE evaluates its assignments against the record returned here, i.e. against the content modify()
   * reloaded, never against the read that went stale, so the record it writes is not the stale read-modify-write the
   * save refuses.
   */
  private static MutableDocument forUpdate(final MutableDocument mutable) {
    mutable.clearBasedOnStaleRead();
    return mutable;
  }
}
