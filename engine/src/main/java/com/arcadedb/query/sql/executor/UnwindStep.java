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

import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Unwind;

import java.util.*;
import java.util.stream.*;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UnwindStep extends AbstractExecutionStep {
  private final Unwind       unwind;
  private final List<String> unwindFields;

  ResultSet        lastResult      = null;
  Iterator<Result> nextSubsequence = null;
  Result           nextElement     = null;

  public UnwindStep(final Unwind unwind, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.unwind = unwind;
    unwindFields = unwind.getItems().stream().map(x -> x.getStringValue()).collect(Collectors.toList());
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("Cannot expand without a target");

    return new ResultSet() {
      long localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(context, nRecords);
        }
        return nextElement != null;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new NoSuchElementException();
        }
        if (nextElement == null) {
          fetchNext(context, nRecords);
        }
        if (nextElement == null) {
          throw new NoSuchElementException();
        }

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
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().get().syncPull(context, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      final Result nextAggregateItem = lastResult.next();
      nextSubsequence = unwind(nextAggregateItem, unwindFields, context).iterator();

    } while (true);

  }

  private Collection<Result> unwind(final Result doc, final List<String> unwindFields, final CommandContext iContext) {
    final List<Result> result = new ArrayList<>();

    if (unwindFields.isEmpty()) {
      result.add(doc);
    } else {
      final String firstField = unwindFields.get(0);
      final List<String> nextFields = unwindFields.subList(1, unwindFields.size());

      final Object fieldValue = doc.getProperty(firstField);
      if (fieldValue == null || fieldValue instanceof Record) {
        result.addAll(unwind(doc, nextFields, iContext));
        return result;
      }

      if (!(fieldValue instanceof Iterable) && !fieldValue.getClass().isArray()) {
        result.addAll(unwind(doc, nextFields, iContext));
        return result;
      }

      final Iterator iterator;
      if (fieldValue.getClass().isArray()) {
        iterator = MultiValue.getMultiValueIterator(fieldValue);
      } else {
        iterator = ((Iterable) fieldValue).iterator();
      }
      if (!iterator.hasNext()) {
        final ResultInternal unwindedDoc = new ResultInternal();
        copy(doc, unwindedDoc);

        unwindedDoc.setProperty(firstField, null);
        result.addAll(unwind(unwindedDoc, nextFields, iContext));
      } else {
        do {
          final Object o = iterator.next();
          final ResultInternal unwindedDoc = new ResultInternal();
          copy(doc, unwindedDoc);
          unwindedDoc.setProperty(firstField, o);
          result.addAll(unwind(unwindedDoc, nextFields, iContext));
        } while (iterator.hasNext());
      }
    }

    return result;
  }

  private void copy(final Result from, final ResultInternal to) {
    for (final String prop : from.getPropertyNames()) {
      to.setProperty(prop, from.getProperty(prop));
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ " + unwind;
  }
}
