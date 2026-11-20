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

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.WhereClause;

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
  // #11818: the WHERE the source selected the records with (null when it has none), and whether the statement is an UPSERT
  private final WhereClause whereClause;
  private final boolean     upsert;

  public ConvertToUpdatableResultStep(final CommandContext context) {
    this(context, null, false);
  }

  public ConvertToUpdatableResultStep(final CommandContext context, final WhereClause whereClause, final boolean upsert) {
    super(context);
    this.whereClause = whereClause;
    this.upsert = upsert;
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
                nextItem = forUpdate((Document) element.getRecord(), context);
                // #11818: a concurrent commit changed the record so that it no longer matches the WHERE
                if (nextItem == null)
                  continue;
              }
              break;
            }

            // Handle projected results containing a RID (e.g. from INSERT ... RETURN @rid)
            final Object ridValue = nextItem.getProperty("@rid");
            if (ridValue instanceof Identifiable identifiable) {
              final Record record = identifiable.getRecord();
              if (record instanceof Document doc) {
                // The WHERE selected the projected row, which holds the @rid and not the record: the record is loaded only now, so
                // there is no earlier read of it to evaluate the WHERE against again
                nextItem = forUpdate(doc, context);
                if (nextItem == null)
                  continue;
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
   * <p>
   * #11818: the WHERE was decided on the content read before modify(), though. When modify() reloaded content that a
   * concurrent transaction changed since that read, the WHERE is evaluated again on it, and a record that no longer
   * matches is not updated, as if the statement had run after that commit (what PostgreSQL does under READ COMMITTED).
   * Updating it anyway built on the concurrent commit, and two "UPDATE ... SET claimed = :me WHERE claimed IS NULL" both
   * claimed the record. An UPSERT that loses its record this way is refused with a retryable conflict instead: skipping it
   * would neither update nor insert, and its retry inserts or updates against the new state.
   *
   * @return the record to update, or null when it no longer matches the WHERE
   */
  private UpdatableResult forUpdate(final Document document, final CommandContext context) {
    final Binary readImage = document instanceof BaseRecord record ? record.getBuffer() : null;
    final MutableDocument mutable = document.modify();
    mutable.clearBasedOnStaleRead();
    final UpdatableResult result = new UpdatableResult(mutable);
    if (whereClause == null || mutable == document || !reloadedWithOtherContent(document, readImage))
      return result;

    // $current is restored: a record that is skipped, or refused, is not the row the statement is at
    final Object oldCurrent = context.getVariable("current");
    context.setVariable("current", result);
    try {
      if (whereClause.matchesFilters(result, context))
        return result;
    } finally {
      context.setVariable("current", oldCurrent);
    }
    if (upsert)
      throw new ConcurrentModificationException("Record " + document.getIdentity()
          + " was modified by a concurrent transaction after the UPDATE UPSERT selected it, and no longer matches its WHERE. "
          + "Please retry the operation");
    return null;
  }

  /**
   * Whether {@code modify()} replaced the content {@code document} held, {@code readImage}, with different bytes: a record
   * whose content was not loaded yet ({@code readImage} null) counts as changed, as nothing tells what the WHERE read.
   */
  private static boolean reloadedWithOtherContent(final Document document, final Binary readImage) {
    if (readImage == null)
      return true;
    final Binary current = ((BaseRecord) document).getBuffer();
    return current != readImage && (current == null || current.size() != readImage.size() || !readImage.isSameRegionAs(0, current, 0,
        readImage.size()));
  }
}
