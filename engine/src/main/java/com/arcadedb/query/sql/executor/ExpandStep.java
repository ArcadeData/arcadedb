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
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Expands a result-set.
 * The pre-requisite is that the input element contains only one field (no matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {
  ResultSet lastResult      = null;
  Iterator  nextSubsequence = null;
  Result    nextElement     = null;

  public ExpandStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (prev == null)
      throw new CommandExecutionException("Cannot expand without a target");

    return new ResultSet() {
      long localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords)
          return false;

        if (nextElement == null)
          fetchNext(context, nRecords);

        return nextElement != null;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords)
          throw new NoSuchElementException();

        if (nextElement == null)
          fetchNext(context, nRecords);

        if (nextElement == null)
          throw new NoSuchElementException();

        final Result result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(context, nRecords);
        return result;
      }

    };
  }

  private void fetchNext(final CommandContext context, final int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final Object nextElementObj = nextSubsequence.next();
          if (nextElementObj instanceof Result) {
            nextElement = (Result) nextElementObj;
          } else if (nextElementObj instanceof Identifiable) {
            final Record record = ((Identifiable) nextElementObj).getRecord();
            if (record == null) {
              continue;
            }
            nextElement = new ResultInternal(record);
          } else if (nextElementObj instanceof Map) {
            nextElement = new ResultInternal((Map) nextElementObj);
          } else {
            nextElement = new ResultInternal();
            ((ResultInternal) nextElement).setProperty("value", nextElementObj);
          }
          break;
        } finally {
          if( context.isProfiling() ) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().syncPull(context, n);
        }
      }

      if (!lastResult.hasNext())
        return;

      final Result nextAggregateItem = lastResult.next();
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        if (nextAggregateItem.getPropertyNames().size() == 0) {
          continue;
        }
        if (nextAggregateItem.getPropertyNames().size() > 1) {
          throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
        }

        final String propName = nextAggregateItem.getPropertyNames().iterator().next();
        final Object projValue = nextAggregateItem.getProperty(propName);
        if (projValue == null) {
          continue;
        }
        if (projValue instanceof Identifiable) {
          final Record rec = ((Identifiable) projValue).getRecord();
          if (rec == null) {
            continue;
          }
          final ResultInternal res = new ResultInternal();
          res.setElement((Document) rec);

          nextSubsequence = Collections.singleton(res).iterator();
        } else if (projValue instanceof Result) {
          nextSubsequence = Collections.singleton((Result) projValue).iterator();
        } else if (projValue instanceof Iterator) {
          nextSubsequence = (Iterator) projValue;
        } else if (projValue instanceof Iterable) {
          nextSubsequence = ((Iterable) projValue).iterator();
        }
      } finally {
        if ( context.isProfiling() )
          cost += (System.nanoTime() - begin);
      }
    } while (true);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXPAND";
    if ( context.isProfiling() )
      result += " (" + getCostFormatted() + ")";

    return result;
  }
}
