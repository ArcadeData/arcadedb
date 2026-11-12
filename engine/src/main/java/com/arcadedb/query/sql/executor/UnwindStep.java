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
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GeneratedAlias;
import com.arcadedb.query.sql.parser.Unwind;

import java.util.*;
import java.util.stream.Collectors;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UnwindStep extends AbstractExecutionStep {
  /**
   * The column under which the projection before the UNWIND carries the record it projected, for an operand that reads
   * both the projected row and the record (issue #11563). The projection after the UNWIND drops it.
   */
  public static final String RECORD_ALIAS = GeneratedAlias.PREFIX + "UNWIND_RECORD$$$";

  /**
   * Checks the statement's deadline and termination while a long list is expanded: the expansion of one input row reads
   * no record, so no scan below this step checks anything meanwhile (issue #9680). Created on the first pull.
   */
  private WorkGuard guard;
  private int       produced = 0;
  private final Unwind       unwind;
  private final List<String> unwindFields;
  /**
   * The operand of each item, parallel to {@link #unwindFields}: null for a bare field read in place, otherwise the
   * expression whose elements are written into the alias (issue #11442).
   */
  private final Expression[] operands;
  /**
   * Parallel to {@link #operands}: whether the operand is evaluated on the projected row laid over the record the row
   * carries under {@link #RECORD_ALIAS} (issue #11563), or null when none is.
   */
  private final boolean[]    overRecord;
  /** The names the projection excluded, which the record must not answer in the row's place. */
  private final Set<String>  hiddenNames;
  ResultSet        lastResult      = null;
  Iterator<Result> nextSubsequence = null;
  Result           nextElement     = null;

  public UnwindStep(final Unwind unwind, final CommandContext context) {
    this(unwind, context, null, null);
  }

  /**
   * @param overRecord  parallel to the items: whether an operand reads the record the row carries under
   *                    {@link #RECORD_ALIAS} for the names the row lacks, or null when none does
   * @param hiddenNames the names the projection excluded, answered from the row (as null) and never from the record
   */
  public UnwindStep(final Unwind unwind, final CommandContext context, final boolean[] overRecord, final Set<String> hiddenNames) {
    super(context);
    this.overRecord = overRecord;
    this.hiddenNames = hiddenNames;
    this.unwind = unwind;
    unwindFields = unwind.getItems().stream().map(x -> x.getStringValue()).collect(Collectors.toList());
    operands = new Expression[unwindFields.size()];
    for (int i = 0; i < operands.length; i++)
      operands[i] = unwind.getExpression(i);
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
    if (guard == null)
      guard = WorkGuard.forCommandDeadline(context);
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().syncPull(context, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      final Result nextAggregateItem = lastResult.next();
      nextSubsequence = unwind(nextAggregateItem, 0, context).iterator();

    } while (true);

  }

  private Collection<Result> unwind(final Result doc, final int itemIndex, final CommandContext context) {
    final List<Result> result = new ArrayList<>();

    if (itemIndex >= unwindFields.size()) {
      result.add(doc);
    } else {
      final String firstField = unwindFields.get(itemIndex);
      final int nextFields = itemIndex + 1;
      final Expression operand = operands[itemIndex];

      final Object fieldValue = operand != null ? operand.execute(operandRow(doc, itemIndex, context), context) : doc.getProperty(firstField);
      if (fieldValue == null || fieldValue instanceof Record
          || (!(fieldValue instanceof Iterable) && !MultiValue.isSequenceArray(fieldValue))) {
        // Null, Record, scalar or BINARY blob value: there is nothing to flatten, so a single row is forwarded.
        // Always forward a defensive copy (never the upstream instance) so that downstream mutations
        // - e.g. a later UNWIND field or a projection step calling setElement(null) - cannot corrupt
        // the shared input record. The unwound property keeps its original value: a scalar behaves
        // like a single-element collection.
        final ResultInternal unwindedDoc = new ResultInternal(context.getDatabase());
        copy(doc, unwindedDoc);
        if (doc.isElement())
          unwindedDoc.setElement(doc.getElement().get());
        if (operand != null)
          // an aliased item writes its single value into the alias, as a bare field already holds it in place
          unwindedDoc.setProperty(firstField, fieldValue);
        result.addAll(unwind(unwindedDoc, nextFields, context));
        return result;
      }

      final Iterator iterator;
      if (MultiValue.isSequenceArray(fieldValue)) {
        iterator = MultiValue.getMultiValueIterator(fieldValue);
      } else {
        iterator = ((Iterable) fieldValue).iterator();
      }
      if (!iterator.hasNext()) {
        final ResultInternal unwindedDoc = new ResultInternal(context.getDatabase());
        copy(doc, unwindedDoc);

        unwindedDoc.setProperty(firstField, null);
        result.addAll(unwind(unwindedDoc, nextFields, context));
      } else {
        do {
          guard.checkPeriodically(++produced);
          final Object o = iterator.next();
          final ResultInternal unwindedDoc = new ResultInternal(context.getDatabase());
          copy(doc, unwindedDoc);
          unwindedDoc.setProperty(firstField, o);
          result.addAll(unwind(unwindedDoc, nextFields, context));
        } while (iterator.hasNext());
      }
    }

    return result;
  }

  /**
   * The row an operand is evaluated on: the row itself, or the row laid over the record it carries when the operand
   * reads both (issue #11563).
   */
  private Result operandRow(final Result doc, final int itemIndex, final CommandContext context) {
    if (overRecord != null && overRecord[itemIndex] && doc.getProperty(RECORD_ALIAS) instanceof Document record)
      return new RecordOverlayResult(context.getDatabase(), doc, record, hiddenNames);
    return doc;
  }

  private void copy(final Result from, final ResultInternal to) {
    for (final String prop : from.getPropertyNames()) {
      to.setProperty(prop, from.getProperty(prop));
    }
    // Copy metadata (e.g., LET variables like $nrid) to preserve them through UNWIND
    for (final String key : from.getMetadataKeys()) {
      to.setMetadata(key, from.getMetadata(key));
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ " + unwind;
  }
}
