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
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.*;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class DeleteFromIndexStep extends AbstractExecutionStep {
  protected final RangeIndex        index;
  private final   BinaryCondition   additional;
  private final   BooleanExpression ridCondition;
  private final   boolean           orderAsc;

  Pair<Object, Identifiable> nextEntry = null;

  final BooleanExpression condition;

  private boolean     initialized = false;
  private IndexCursor cursor;

  public DeleteFromIndexStep(final RangeIndex index, final BooleanExpression condition,
                             final BinaryCondition additionalRangeCondition,
                             final BooleanExpression ridCondition, final CommandContext context) {
    this(index, condition, additionalRangeCondition, ridCondition, true, context);
  }

  public DeleteFromIndexStep(final RangeIndex index, final BooleanExpression condition,
                             final BinaryCondition additionalRangeCondition,
                             final BooleanExpression ridCondition, final boolean orderAsc,
                             final CommandContext context) {
    super(context);
    this.index = index;
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init();

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        return localCount < nRecords && nextEntry != null;
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (!hasNext())
            throw new NoSuchElementException();

          final Pair<Object, Identifiable> entry = nextEntry;
          final ResultInternal result = new ResultInternal(context.getDatabase());
          final Identifiable value = entry.getSecond();

          index.remove(new Object[]{entry.getFirst()}, value);
          localCount++;
          nextEntry = loadNextEntry(context);
          return result;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

    };
  }

  /**
   * #5662: an index scan that fails to initialise aborts the step. It used to print the stack trace to stdout and carry
   * on with a null cursor, so the operator would see a {@link NullPointerException} out of {@code loadNextEntry()}
   * instead of the {@link IOException} that said what had actually gone wrong. No branch of {@link #init(BooleanExpression)}
   * throws it today - the catch is there because the method declares it - so this is about what happens the day one
   * does, not about a failure reachable now.
   */
  private synchronized void init() {
    if (initialized) {
      return;
    }
    initialized = true;
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      // Removing an index entry directly never goes through the bucket's own DELETE_RECORD check, so apply it here.
      SecurityHelper.checkAccessOnIndex((DatabaseInternal) context.getDatabase(), index, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
      init(condition);
      nextEntry = loadNextEntry(context);
    } catch (final IOException e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Error on initializing the scan of index '%s' to delete from", e, index.getName());
      releaseCursor();
      throw new CommandExecutionException("Error on initializing the scan of index '" + index.getName() + "' to delete from", e);
    } catch (final RuntimeException e) {
      // an abort AFTER the cursor was opened - an unsupported condition, or a page read failing inside
      // loadNextEntry() - must not depend on the executor getting around to close(): release it here
      releaseCursor();
      throw e;
    } finally {
      if (context.isProfiling()) {
        cost += System.nanoTime() - begin;
      }
    }
  }

  @Override
  public void close() {
    releaseCursor();
    super.close();
  }

  /**
   * #5662: releases the index cursor. A DELETE stops as soon as its {@code LIMIT} is reached, so the cursor is
   * routinely abandoned partway - and an abandoned compacted-series cursor stays registered with its file, which
   * {@code LSMTreeIndex.dropRetiredCompactedIndexes} then refuses to drop for the lifetime of the database.
   */
  private void releaseCursor() {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    // a closed step is finished: without this the result set handed out by syncPull would still answer hasNext() and
    // then dereference the released cursor
    nextEntry = null;
  }

  private Pair<Object, Identifiable> loadNextEntry(final CommandContext commandContext) {
    while (cursor.hasNext()) {
      final Object value = cursor.next();

      final Pair<Object, Identifiable> result = new Pair(cursor.getKeys(), value);
      if (ridCondition == null)
        return result;

      final ResultInternal res = new ResultInternal(context.getDatabase());
      res.setProperty("rid", result.getSecond());
      if (ridCondition.evaluate(res, commandContext))
        return result;

    }
    return null;
  }

  /**
   * #5682: {@code condition} is built by {@code DeleteExecutionPlanner.getKeyCondition()}, the only caller that
   * constructs a {@link DeleteFromIndexStep}. That method always returns one flattened sub-block of an
   * {@code AndBlock}, never the {@code AndBlock} itself, so an {@code AndBlock} can never reach here through SQL.
   * It falls through to the same "not supported yet" rejection as any other condition shape this step doesn't
   * handle, rather than getting a dedicated branch that no test could ever exercise.
   */
  private void init(final BooleanExpression condition) throws IOException {
    if (condition == null) {
      processFlatIteration();
    } else if (condition instanceof BinaryCondition) {
      processBinaryCondition();
    } else if (condition instanceof BetweenCondition) {
      processBetweenCondition();
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  private void processFlatIteration() {
    cursor = index.iterator(isOrderAsc());
  }

  private void processBetweenCondition() {
    final Expression key = ((BetweenCondition) condition).getFirst();
    if (!"key".equalsIgnoreCase(key.toString())) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Expression second = ((BetweenCondition) condition).getSecond();
    final Expression third = ((BetweenCondition) condition).getThird();

    final Object secondValue = second.execute((Result) null, context);
    final Object thirdValue = third.execute((Result) null, context);
    if (isOrderAsc())
      cursor = index.range(true, new Object[]{secondValue}, true, new Object[]{thirdValue}, true);
    else
      cursor = index.range(false, new Object[]{thirdValue}, true, new Object[]{secondValue}, true);
  }

  private void processBinaryCondition() {
    final BinaryCompareOperator operator = ((BinaryCondition) condition).getOperator();
    final Expression left = ((BinaryCondition) condition).getLeft();
    if (!"key".equalsIgnoreCase(left.toString())) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Object rightValue = ((BinaryCondition) condition).getRight().execute((Result) null, context);
    cursor = createCursor(operator, rightValue);
  }

  private IndexCursor createCursor(final BinaryCompareOperator operator, final Object value) {
    final boolean orderAsc = isOrderAsc();
    if (operator instanceof EqualsCompareOperator) {
      return index.iterator(orderAsc, new Object[]{value}, true);
    } else if (operator instanceof GeOperator) {
      return index.iterator(orderAsc, new Object[]{value}, true);
    } else if (operator instanceof GtOperator) {
      return index.iterator(orderAsc, new Object[]{value}, false);
    } else if (operator instanceof LeOperator) {
      return index.iterator(orderAsc, new Object[]{value}, true);
    } else if (operator instanceof LtOperator) {
      return index.iterator(orderAsc, new Object[]{value}, false);
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    result += condition == null ?
        "" :
        ("\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additional == null ?
            "" :
            " and " + additional));
    return result;
  }

}
