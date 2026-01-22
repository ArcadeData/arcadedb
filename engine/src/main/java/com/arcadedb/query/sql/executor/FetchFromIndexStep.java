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

import com.arcadedb.database.Database;
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
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.IsNullCondition;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.PCollection;
import com.arcadedb.query.sql.parser.ValueExpression;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Created by luigidellaquila on 23/07/16.
 */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected final String                                         indexName;
  private final   List<IndexCursor>                              nextCursors = new ArrayList<>();
  protected       RangeIndex                                     index;
  protected       BooleanExpression                              condition;
  private         BinaryCondition                                additionalRangeCondition;
  private         boolean                                        orderAsc;
  private         long                                           count       = 0;
  private         boolean                                        inited      = false;
  private         IndexCursor                                    cursor;
  private         MultiIterator<Map.Entry<Object, Identifiable>> customIterator;
  private         Iterator<?>                                    nullKeyIterator;
  private         Pair<Object, Identifiable>                     nextEntry   = null;
  private         int                                            nextEntryScore = 0;

  public FetchFromIndexStep(final RangeIndex index, final BooleanExpression condition,
      final BinaryCondition additionalRangeCondition,
      final CommandContext context) {
    this(index, condition, additionalRangeCondition, true, context);
  }

  public FetchFromIndexStep(final RangeIndex index, final BooleanExpression condition,
      final BinaryCondition additionalRangeCondition, final boolean orderAsc,
      final CommandContext context) {
    super(context);
    this.index = index;
    this.indexName = index.getName();
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    init(context.getDatabase());

    pullPrevious(context, nRecords);

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords)
          return false;

        if (nextEntry == null)
          fetchNextEntry();

        return nextEntry != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final Object key = nextEntry.getFirst();
          final Identifiable value = nextEntry.getSecond();

          nextEntry = null;

          localCount++;
          final ResultInternal result = new ResultInternal();
          result.setProperty("key", key);
          result.setProperty("rid", value);
          if (nextEntryScore > 0)
            result.setProperty("$score", nextEntryScore);
          context.setVariable("current", result);
          return result;
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }
    };
  }

  private void fetchNextEntry() {
    nextEntry = null;
    while (true) {
      if (cursor == null) {
        if (nextCursors.isEmpty()) {
          if (nextEntry == null && customIterator != null && customIterator.hasNext()) {
            final Map.Entry<Object, Identifiable> entry = customIterator.next();
            nextEntry = new Pair<>(entry.getKey(), entry.getValue().getIdentity());
            nextEntryScore = 0;
          }
          if (nextEntry == null && nullKeyIterator != null && nullKeyIterator.hasNext()) {
            Identifiable nextValue = (Identifiable) nullKeyIterator.next();
            nextEntry = new Pair<>(null, nextValue.getIdentity());
            nextEntryScore = 0;
          }

          if (nextEntry == null)
            updateIndexStats();
          else
            count++;

          return;
        }
        cursor = nextCursors.removeFirst();
      }
      if (cursor.hasNext()) {
        final Object value = cursor.next();
        nextEntry = new Pair(cursor.getKeys(), value);
        nextEntryScore = cursor.getScore();
        count++;
        return;
      }

      cursor = null;
    }
  }

  private void updateIndexStats() {
    //stats
    final QueryStats stats = QueryStats.get(context.getDatabase());
    if (index == null) {
      return;//this could happen, if not inited yet
    }
    final String indexName = index.getName();
    boolean range = false;
    int size = 0;

    if (condition instanceof BinaryCondition) {
      size = 1;
    } else if (condition instanceof BetweenCondition) {
      size = 1;
      range = true;
    } else if (condition instanceof AndBlock block) {
      final AndBlock andBlock = block;
      size = andBlock.getSubBlocks().size();
      final BooleanExpression lastOp = andBlock.getSubBlocks().get(andBlock.getSubBlocks().size() - 1);
      if (lastOp instanceof BinaryCondition binaryCondition) {
        final BinaryCompareOperator op = binaryCondition.getOperator();
        range = op.isRangeOperator();
      }
    } else if (condition instanceof InCondition) {
      size = 1;
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count);
  }

  private synchronized void init(final Database db) {
    if (inited) {
      return;
    }
    inited = true;
    init(condition, db);
  }

  private void init(final BooleanExpression condition, final Database db) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    if (index == null) {
      index = (RangeIndex) db.getSchema().getIndexByName(indexName);
    }
    try {
      if (condition == null) {
        processFlatIteration();
      } else if (condition instanceof BinaryCondition) {
        processBinaryCondition();
      } else if (condition instanceof BetweenCondition) {
        processBetweenCondition();
      } else if (condition instanceof AndBlock) {
        processAndBlock();
      } else if (condition instanceof InCondition) {
        processInCondition();
      } else if (condition instanceof IsNullCondition) {
        processIsNullCondition();
      } else {
        //TODO process containsAny
        throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
      }
    } finally {
      if (context.isProfiling()) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private void processInCondition() {
    final InCondition inCondition = (InCondition) condition;

    final Expression left = inCondition.getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Object rightValue = inCondition.evaluateRight((Result) null, context);
    final EqualsCompareOperator equals = new EqualsCompareOperator(-1);
    if (MultiValue.isMultiValue(rightValue)) {
      customIterator = new MultiIterator<>();
      for (final Object item : MultiValue.getMultiValueIterable(rightValue)) {
        final IndexCursor localCursor = createCursor(equals, item, context);

        customIterator.addIterator(new Iterator<Map.Entry>() {
          @Override
          public boolean hasNext() {
            return localCursor.hasNext();
          }

          @Override
          public Map.Entry next() {
            if (!localCursor.hasNext()) {
              throw new NoSuchElementException();
            }
            final Identifiable value = localCursor.next();
            return new Map.Entry() {

              @Override
              public Object getKey() {
                return item;
              }

              @Override
              public Object getValue() {
                return value;
              }

              @Override
              public Object setValue(final Object value) {
                return null;
              }
            };
          }
        });
      }
      customIterator.reset();
    } else {
      cursor = createCursor(equals, rightValue, context);
    }
    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    final PCollection fromKey = indexKeyFrom((AndBlock) condition, additionalRangeCondition);
    final PCollection toKey = indexKeyTo((AndBlock) condition, additionalRangeCondition);
    final boolean fromKeyIncluded = indexKeyFromIncluded((AndBlock) condition, additionalRangeCondition);
    final boolean toKeyIncluded = indexKeyToIncluded((AndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processIsNullCondition() {
    final int keyCount = index.getPropertyNames().size();
    final Object[] nullKeys = new Object[keyCount];
    cursor = index.get(nullKeys);
    fetchNextEntry();
  }

  private void processFlatIteration() {
    cursor = index.iterator(isOrderAsc());

    fetchNullKeys();
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private void fetchNullKeys() {
    if (index.getNullStrategy() != LSMTreeIndexAbstract.NULL_STRATEGY.INDEX) {
      nullKeyIterator = Collections.emptyIterator();
      return;
    }
    final int keyCount = index.getPropertyNames().size();
    final Object[] nullKeys = new Object[keyCount];
    final IndexCursor nullCursor = index.get(nullKeys);
    nullKeyIterator = new Iterator<>() {
      @Override
      public boolean hasNext() {
        return nullCursor.hasNext();
      }

      @Override
      public Identifiable next() {
        return nullCursor.next();
      }
    };
  }

  private void init(final PCollection fromKey, final boolean fromKeyIncluded, final PCollection toKey,
      final boolean toKeyIncluded) {
    final List<PCollection> secondValueCombinations = cartesianProduct(fromKey);
    final List<PCollection> thirdValueCombinations = cartesianProduct(toKey);

    for (int i = 0; i < secondValueCombinations.size(); i++) {

      Object secondValue = secondValueCombinations.get(i).execute((Result) null, context);
      Object thirdValue = thirdValueCombinations.get(i).execute((Result) null, context);

      secondValue = convertToIndexDefinitionTypes(secondValue);
      thirdValue = convertToIndexDefinitionTypes(thirdValue);
      final IndexCursor cursor;

      Object[] convertedFrom = convertToObjectArray(secondValue);
      if (convertedFrom.length == 0)
        convertedFrom = null;
      Object[] convertedTo = convertToObjectArray(thirdValue);
      if (convertedTo.length == 0)
        convertedTo = null;

      if (Arrays.equals(convertedFrom, convertedTo) && fromKeyIncluded && toKeyIncluded
          && convertedFrom != null && index.getPropertyNames().size() == convertedFrom.length)
        cursor = index.get(convertedFrom);
      else if (index.supportsOrderedIterations()) {
        if (orderAsc)
          cursor = index.range(true, convertedFrom, fromKeyIncluded, convertedTo, toKeyIncluded);
        else
          cursor = index.range(false, convertedTo, toKeyIncluded, convertedFrom, fromKeyIncluded);
      } else if (additionalRangeCondition == null && allEqualities((AndBlock) condition)) {
        cursor = index.iterator(isOrderAsc(), convertedFrom, true);
      } else {
        throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
      }
      nextCursors.add(cursor);

    }
    if (nextCursors.size() > 0) {
      cursor = nextCursors.removeFirst();
      fetchNextEntry();
    }
  }

  private Object[] convertToObjectArray(final Object value) {
    final Object[] result;

    if (value instanceof Object[] objects)
      result = objects;
    else if (value instanceof Collection collection)
      result = collection.toArray();
    else
      result = new Object[] { value };

    return result;
  }

  private List<PCollection> cartesianProduct(final PCollection key) {
    return cartesianProduct(new PCollection(-1), key);//TODO
  }

  private List<PCollection> cartesianProduct(final PCollection head, final PCollection key) {
    if (key.getExpressions().isEmpty())
      return List.of(head);

    final Expression nextElementInKey = key.getExpressions().getFirst();
    final Object value = nextElementInKey.execute(new ResultInternal(context.getDatabase()), context);
    if (value instanceof Iterable<?> iterable && !(value instanceof Identifiable)) {
      final List<PCollection> result = new ArrayList<>();
      for (final Object elemInKey : iterable) {
        final PCollection newHead = new PCollection(-1);
        for (final Expression exp : head.getExpressions())
          newHead.add(exp.copy());

        newHead.add(toExpression(elemInKey));
        final PCollection tail = key.copy();
        tail.getExpressions().removeFirst();
        result.addAll(cartesianProduct(newHead, tail));
      }
      return result;
    } else {
      final PCollection newHead = new PCollection(-1);
      for (final Expression exp : head.getExpressions())
        newHead.add(exp.copy());

      newHead.add(nextElementInKey);
      final PCollection tail = key.copy();
      tail.getExpressions().removeFirst();
      return cartesianProduct(newHead, tail);
    }

  }

  private Expression toExpression(final Object value) {
    return new ValueExpression(value);
  }

  private Object convertToIndexDefinitionTypes(final Object val/*, OType[] types*/) {
    //TODO
    return val;

//    if (val == null) {
//      return null;
//    }
//    if (OMultiValue.isMultiValue(val)) {
//      List<Object> result = new ArrayList<>();
//      int i = 0;
//      for (Object o : OMultiValue.getMultiValueIterable(val)) {
//        result.add(OType.convert(o, types[i++].getDefaultJavaType()));
//      }
//      return result;
//    }
//    return OType.convert(val, types[0].getDefaultJavaType());
  }

  private boolean allEqualities(final AndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (final BooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof BinaryCondition binaryCondition) {
        if (binaryCondition.getOperator() instanceof EqualsCompareOperator) {
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
    if (!key.toString().equalsIgnoreCase("key"))
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");

    final Expression second = ((BetweenCondition) condition).getSecond();
    final Expression third = ((BetweenCondition) condition).getThird();

    final Object secondValue = second.execute((Result) null, context);
    final Object thirdValue = third.execute((Result) null, context);
    if (isOrderAsc())
      cursor = index.range(true, new Object[] { secondValue }, true, new Object[] { thirdValue }, true);
    else
      cursor = index.range(false, new Object[] { thirdValue }, true, new Object[] { secondValue }, true);

    if (cursor != null)
      fetchNextEntry();
  }

  private void processBinaryCondition() {
    final BinaryCompareOperator operator = ((BinaryCondition) condition).getOperator();
    final Expression left = ((BinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Object rightValue = ((BinaryCondition) condition).getRight().execute((Result) null, context);
    cursor = createCursor(operator, rightValue, context);
    if (cursor != null) {
      fetchNextEntry();
    }
  }

//  private Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      rightValue = ((Collection) rightValue).iterator().next();
//    }
//    if (rightValue instanceof List) {
//      rightValue = definition.createValue((List<?>) rightValue);
//    } else if (!(rightValue instanceof OCompositeKey)) {
//      rightValue = definition.createValue(rightValue);
//    }
//    if (!(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return (Collection) rightValue;
//  }

  private Object[] toBetweenIndexKey(final Index definition, final Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      if (((Collection) rightValue).size() > 0) {
//        rightValue = ((Collection) rightValue).iterator().next();
//      } else {
//        rightValue = null;
//      }
//    }
//    rightValue = definition.createValue(rightValue);
//
//    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return rightValue;
    throw new UnsupportedOperationException();
  }

  private IndexCursor createCursor(final BinaryCompareOperator operator, final Object value, final CommandContext context) {
    // TODO: WHAT TO DO WITH ASC ORDER?

    final Object[] values;
    if (!(value instanceof Object[]))
      values = new Object[] { value };
    else
      values = (Object[]) value;

    if (operator instanceof EqualsCompareOperator) {
      return index.get(values);
    } else if (operator instanceof GeOperator) {
      return index.iterator(true, values, true);
    } else if (operator instanceof GtOperator) {
      return index.iterator(true, values, false);
    } else if (operator instanceof LeOperator) {
      return index.iterator(false, values, true);
    } else if (operator instanceof LtOperator) {
      return index.iterator(false, values, false);
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private static PCollection indexKeyFrom(final AndBlock keyCondition, final BinaryCondition additional) {
    PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      Expression res = exp.resolveKeyFrom(additional);
      if (res != null) {
        result.add(res);
      }
    }
    return result;
  }

  private static PCollection indexKeyTo(final AndBlock keyCondition, final BinaryCondition additional) {
    PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      Expression res = exp.resolveKeyTo(additional);
      if (res != null) {
        result.add(res);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(final AndBlock keyCondition, final BinaryCondition additional) {
    final BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    return exp.isKeyFromIncluded(additional);
  }

  private boolean indexKeyToIncluded(final AndBlock keyCondition, final BinaryCondition additional) {
    final BooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    return exp.isKeyToIncluded(additional);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX " + indexName;
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    if (condition != null) {
      result += ("\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additionalRangeCondition == null ?
          "" :
          " and " + additionalRangeCondition));
    }

    return result;
  }

  @Override
  public void reset() {
    index = null;
    condition = condition == null ? null : condition.copy();
    additionalRangeCondition = additionalRangeCondition == null ? null : additionalRangeCondition.copy();

    cost = 0;
    count = 0;

    inited = false;
    cursor = null;
    customIterator = null;
    nullKeyIterator = null;
    nextEntry = null;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FetchFromIndexStep(index, this.condition == null ? null : this.condition.copy(),
        this.additionalRangeCondition == null ? null : this.additionalRangeCondition.copy(), this.orderAsc, context);
  }
}
