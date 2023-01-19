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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.PCollection;
import com.arcadedb.utility.Pair;

import java.io.*;
import java.util.*;

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

  private boolean     inited = false;
  private IndexCursor cursor;



  public DeleteFromIndexStep(final RangeIndex index, final BooleanExpression condition, final BinaryCondition additionalRangeCondition, final BooleanExpression ridCondition,
      final CommandContext ctx, final boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  public DeleteFromIndexStep(final RangeIndex index, final BooleanExpression condition, final BinaryCondition additionalRangeCondition, final BooleanExpression ridCondition,
      final boolean orderAsc, final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        return (localCount < nRecords && nextEntry != null);
      }

      @Override
      public Result next() {
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          final Pair<Object, Identifiable> entry = nextEntry;
          final ResultInternal result = new ResultInternal();
          final Identifiable value = entry.getSecond();

          index.remove(new Object[] { entry.getFirst() }, value);
          localCount++;
          nextEntry = loadNextEntry(ctx);
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }




    };
  }

  private synchronized void init() {
    if (inited) {
      return;
    }
    inited = true;
    final long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      init(condition);
      nextEntry = loadNextEntry(ctx);
    } catch (final IOException e) {
      e.printStackTrace();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private Pair<Object, Identifiable> loadNextEntry(final CommandContext commandContext) {
    while (cursor.hasNext()) {
      final Object value = cursor.next();

      final Pair<Object, Identifiable> result = new Pair(cursor.getKeys(), value);
      if (ridCondition == null) {
        return result;
      }
      final ResultInternal res = new ResultInternal();
      res.setProperty("rid", result.getSecond());
      if (ridCondition.evaluate(res, commandContext)) {
        return result;
      }
    }
    return null;
  }

  private void init(final BooleanExpression condition) throws IOException {
    if (condition == null) {
      processFlatIteration();
    } else if (condition instanceof BinaryCondition) {
      processBinaryCondition();
    } else if (condition instanceof BetweenCondition) {
      processBetweenCondition();
    } else if (condition instanceof AndBlock) {
      processAndBlock();
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    final PCollection fromKey = indexKeyFrom((AndBlock) condition, additional);
    final PCollection toKey = indexKeyTo((AndBlock) condition, additional);
    final boolean fromKeyIncluded = indexKeyFromIncluded((AndBlock) condition, additional);
    final boolean toKeyIncluded = indexKeyToIncluded((AndBlock) condition, additional);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() {
    cursor = index.iterator(isOrderAsc());
  }

  private void init(final PCollection fromKey, final boolean fromKeyIncluded, final PCollection toKey, final boolean toKeyIncluded) {
    final Object secondValue = fromKey.execute((Result) null, ctx);
    final Object thirdValue = toKey.execute((Result) null, ctx);

    if (index.supportsOrderedIterations()) {
      if (isOrderAsc())
        cursor = index.range(true, new Object[] { secondValue }, fromKeyIncluded, new Object[] { thirdValue }, toKeyIncluded);
      else
        cursor = index.range(false, new Object[] { thirdValue }, fromKeyIncluded, new Object[] { secondValue }, toKeyIncluded);
    } else if (additional == null && allEqualities((AndBlock) condition)) {

      if (isOrderAsc())
        cursor = index.range(true, new Object[] { secondValue }, fromKeyIncluded, new Object[] { thirdValue }, toKeyIncluded);
      else
        cursor = index.range(false, new Object[] { thirdValue }, fromKeyIncluded, new Object[] { secondValue }, toKeyIncluded);

      cursor = index.iterator(isOrderAsc(), new Object[] { secondValue }, true);
    } else {
      throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
    }
  }

  private boolean allEqualities(final AndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (final BooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        if (((BinaryCondition) exp).getOperator() instanceof EqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition() {
    final Expression key = ((BetweenCondition) condition).getFirst();
    if (!key.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Expression second = ((BetweenCondition) condition).getSecond();
    final Expression third = ((BetweenCondition) condition).getThird();

    final Object secondValue = second.execute((Result) null, ctx);
    final Object thirdValue = third.execute((Result) null, ctx);
    if (isOrderAsc())
      cursor = index.range(true, new Object[] { secondValue }, true, new Object[] { thirdValue }, true);
    else
      cursor = index.range(false, new Object[] { thirdValue }, true, new Object[] { secondValue }, true);
  }

  private void processBinaryCondition() {
    final BinaryCompareOperator operator = ((BinaryCondition) condition).getOperator();
    final Expression left = ((BinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Object rightValue = ((BinaryCondition) condition).getRight().execute((Result) null, ctx);
    cursor = createCursor(operator, index, rightValue, ctx);
  }

  private IndexCursor createCursor(final BinaryCompareOperator operator, final Index definition, final Object value, final CommandContext ctx) {
    final boolean orderAsc = isOrderAsc();
    if (operator instanceof EqualsCompareOperator) {
      return index.iterator(orderAsc, new Object[] { value }, true);
    } else if (operator instanceof GeOperator) {
      return index.iterator(orderAsc, new Object[] { value }, true);
    } else if (operator instanceof GtOperator) {
      return index.iterator(orderAsc, new Object[] { value }, false);
    } else if (operator instanceof LeOperator) {
      return index.iterator(orderAsc, new Object[] { value }, true);
    } else if (operator instanceof LtOperator) {
      return index.iterator(orderAsc, new Object[] { value }, false);
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }

  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private PCollection indexKeyFrom(final AndBlock keyCondition, final BinaryCondition additional) {
    final PCollection result = new PCollection(-1);
    for (final BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        final BinaryCondition binaryCond = ((BinaryCondition) exp);
        final BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof GtOperator) || (operator instanceof GeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private PCollection indexKeyTo(final AndBlock keyCondition, final BinaryCondition additional) {
    final PCollection result = new PCollection(-1);
    for (final BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        final BinaryCondition binaryCond = ((BinaryCondition) exp);
        final BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof LtOperator) || (operator instanceof LeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(final AndBlock keyCondition, final BinaryCondition additional) {
    final BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof BinaryCondition) {
      final BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      final BinaryCompareOperator additionalOperator = additional == null ? null : additional.getOperator();
      if (isGreaterOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private boolean isGreaterOperator(final BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof GeOperator || operator instanceof GtOperator;
  }

  private boolean isLessOperator(final BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof LeOperator || operator instanceof LtOperator;
  }

  private boolean isIncludeOperator(final BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof GeOperator || operator instanceof LeOperator;
  }

  private boolean indexKeyToIncluded(final AndBlock keyCondition, final BinaryCondition additional) {
    final BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof BinaryCondition) {
      final BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      final BinaryCompareOperator additionalOperator = additional == null ? null : additional.getOperator();
      if (isLessOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (condition == null ?
        "" :
        ("\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additional == null ? "" : " and " + additional)));
    return result;
  }


}
