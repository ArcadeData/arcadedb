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
import com.arcadedb.query.sql.parser.*;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Created by luigidellaquila on 23/07/16.
 */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected     RangeIndex        index;
  protected     BooleanExpression condition;
  private       BinaryCondition   additionalRangeCondition;
  private       boolean           orderAsc;
  protected     String            indexName;
  private       long              cost        = 0;
  private       long              count       = 0;
  private       boolean           inited      = false;
  private       IndexCursor       cursor;
  private final List<IndexCursor> nextCursors = new ArrayList<>();

  private MultiIterator<Map.Entry<Object, Identifiable>> customIterator;

  private Iterator                   nullKeyIterator;
  private Pair<Object, Identifiable> nextEntry = null;

  public FetchFromIndexStep(RangeIndex index, BooleanExpression condition, BinaryCondition additionalRangeCondition, CommandContext ctx,
      boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, true, ctx, profilingEnabled);
  }

  public FetchFromIndexStep(RangeIndex index, BooleanExpression condition, BinaryCondition additionalRangeCondition, boolean orderAsc, CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.indexName = index.getName();
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;

    this.orderAsc = orderAsc;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    init(ctx.getDatabase());
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextEntry == null) {
          fetchNextEntry();
        }
        return nextEntry != null;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Object key = nextEntry.getFirst();
          Identifiable value = nextEntry.getSecond();

          nextEntry = null;

          localCount++;
          ResultInternal result = new ResultInternal();
          result.setProperty("key", key);
          result.setProperty("rid", value);
          ctx.setVariable("$current", result);
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }

        }
      }

      @Override
      public void close() {
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNextEntry() {
    nextEntry = null;
    while (true) {
      if (cursor == null) {
        if (nextCursors.size() == 0) {
          if (nextEntry == null && nullKeyIterator != null && nullKeyIterator.hasNext()) {
            Identifiable nextValue = (Identifiable) nullKeyIterator.next();
            nextEntry = new Pair(null, nextValue);
          } else {
            updateIndexStats();
          }
          return;
        }
        cursor = nextCursors.remove(0);
      }
      if (cursor.hasNext()) {
        final Object value = cursor.next();
        nextEntry = new Pair(cursor.getKeys(), value);
        count++;
        return;
      }

      cursor = null;
    }
  }

  private void updateIndexStats() {
    //stats
    QueryStats stats = QueryStats.get(ctx.getDatabase());
    if (index == null) {
      return;//this could happen, if not inited yet
    }
    String indexName = index.getName();
    boolean range = false;
    int size = 0;

    if (condition instanceof BinaryCondition) {
      size = 1;
    } else if (condition instanceof BetweenCondition) {
      size = 1;
      range = true;
    } else if (condition instanceof AndBlock) {
      AndBlock andBlock = ((AndBlock) condition);
      size = andBlock.getSubBlocks().size();
      BooleanExpression lastOp = andBlock.getSubBlocks().get(andBlock.getSubBlocks().size() - 1);
      if (lastOp instanceof BinaryCondition) {
        BinaryCompareOperator op = ((BinaryCondition) lastOp).getOperator();
        range = op.isRangeOperator();
      }
    } else if (condition instanceof InCondition) {
      size = 1;
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count);
  }

  private synchronized void init(Database db) {
    if (inited) {
      return;
    }
    inited = true;
    init(condition, db);
  }

  private void init(BooleanExpression condition, Database db) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
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
      } else {
        //TODO process containsAny
        throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
      }
    } catch (IOException e) {
      throw new CommandExecutionException(e);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private void processInCondition() throws IOException {
    final InCondition inCondition = (InCondition) condition;

    Expression left = inCondition.getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    Object rightValue = inCondition.evaluateRight((Result) null, ctx);
    EqualsCompareOperator equals = new EqualsCompareOperator(-1);
    if (MultiValue.isMultiValue(rightValue)) {
      customIterator = new MultiIterator<>();
      for (Object item : MultiValue.getMultiValueIterable(rightValue)) {
        IndexCursor localCursor = createCursor(equals, item, ctx);

        customIterator.addIterator(new Iterator<Map.Entry>() {
          @Override
          public boolean hasNext() {
            return localCursor.hasNext();
          }

          @Override
          public Map.Entry next() {
            if (!localCursor.hasNext()) {
              throw new IllegalStateException();
            }
            Identifiable value = localCursor.next();
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
              public Object setValue(Object value) {
                return null;
              }
            };
          }
        });
      }
      customIterator.reset();
    } else {
      cursor = createCursor(equals, rightValue, ctx);
    }
    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() throws IOException {
    PCollection fromKey = indexKeyFrom((AndBlock) condition, additionalRangeCondition);
    PCollection toKey = indexKeyTo((AndBlock) condition, additionalRangeCondition);
    boolean fromKeyIncluded = indexKeyFromIncluded((AndBlock) condition, additionalRangeCondition);
    boolean toKeyIncluded = indexKeyToIncluded((AndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() throws IOException {
    cursor = index.iterator(isOrderAsc());

    fetchNullKeys();
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private void fetchNullKeys() {
//    if (index.getDefinition().isNullValuesIgnored()) {
    nullKeyIterator = Collections.emptyIterator();
//      return;
//    }
//    Object nullIter = index.get(null);
//    if (nullIter instanceof PIdentifiable) {
//      nullKeyIterator = Collections.singleton(nullIter).iterator();
//    } else if (nullIter instanceof Iterable) {
//      nullKeyIterator = ((Iterable) nullIter).iterator();
//    } else if (nullIter instanceof Iterator) {
//      nullKeyIterator = (Iterator) nullIter;
//    } else {
//      nullKeyIterator = Collections.emptyIterator();
//    }
  }

  private void init(PCollection fromKey, boolean fromKeyIncluded, PCollection toKey, boolean toKeyIncluded) {
    List<PCollection> secondValueCombinations = cartesianProduct(fromKey);
    List<PCollection> thirdValueCombinations = cartesianProduct(toKey);

    for (int i = 0; i < secondValueCombinations.size(); i++) {

      Object secondValue = secondValueCombinations.get(i).execute((Result) null, ctx);
      Object thirdValue = thirdValueCombinations.get(i).execute((Result) null, ctx);

      secondValue = convertToIndexDefinitionTypes(secondValue);
      thirdValue = convertToIndexDefinitionTypes(thirdValue);
      IndexCursor cursor;

      final Object[] converted = convertToObjectArray(secondValue);

      if (secondValue.equals(thirdValue) && fromKeyIncluded && toKeyIncluded && index.getPropertyNames().size() == converted.length)
        cursor = index.get(converted);
      else if (index.supportsOrderedIterations()) {
        cursor = index.range(isOrderAsc(), converted, fromKeyIncluded, convertToObjectArray(thirdValue), toKeyIncluded);
      } else if (additionalRangeCondition == null && allEqualities((AndBlock) condition)) {
        cursor = index.iterator(isOrderAsc(), converted, true);
      } else {
        throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
      }
      nextCursors.add(cursor);

    }
    if (nextCursors.size() > 0) {
      cursor = nextCursors.remove(0);
      fetchNextEntry();
    }
  }

  private Object[] convertToObjectArray(final Object value) {
    final Object[] result;

    if (value instanceof Object[])
      result = (Object[]) value;
    else if (value instanceof Collection)
      result = ((Collection) value).toArray();
    else
      result = new Object[] { value };

    return result;
  }

  private List<PCollection> cartesianProduct(PCollection key) {
    return cartesianProduct(new PCollection(-1), key);//TODO
  }

  private List<PCollection> cartesianProduct(PCollection head, PCollection key) {
    if (key.getExpressions().size() == 0) {
      return Collections.singletonList(head);
    }
    Expression nextElementInKey = key.getExpressions().get(0);
    Object value = nextElementInKey.execute(new ResultInternal(), ctx);
    if (value instanceof Iterable && !(value instanceof Identifiable)) {
      List<PCollection> result = new ArrayList<>();
      for (Object elemInKey : (Iterable<?>) value) {
        PCollection newHead = new PCollection(-1);
        for (Expression exp : head.getExpressions()) {
          newHead.add(exp.copy());
        }
        newHead.add(toExpression(elemInKey));
        PCollection tail = key.copy();
        tail.getExpressions().remove(0);
        result.addAll(cartesianProduct(newHead, tail));
      }
      return result;
    } else {
      PCollection newHead = new PCollection(-1);
      for (Expression exp : head.getExpressions()) {
        newHead.add(exp.copy());
      }
      newHead.add(nextElementInKey);
      PCollection tail = key.copy();
      tail.getExpressions().remove(0);
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
    for (BooleanExpression exp : condition.getSubBlocks()) {
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
    if (!key.toString().equalsIgnoreCase("key"))
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");

    final Expression second = ((BetweenCondition) condition).getSecond();
    final Expression third = ((BetweenCondition) condition).getThird();

    final Object secondValue = second.execute((Result) null, ctx);
    final Object thirdValue = third.execute((Result) null, ctx);
    if (isOrderAsc())
      cursor = index.range(true, new Object[] { secondValue }, true, new Object[] { thirdValue }, true);
    else
      cursor = index.range(false, new Object[] { thirdValue }, true, new Object[] { secondValue }, true);

    if (cursor != null)
      fetchNextEntry();
  }

  private void processBinaryCondition() {
    BinaryCompareOperator operator = ((BinaryCondition) condition).getOperator();
    Expression left = ((BinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((BinaryCondition) condition).getRight().execute((Result) null, ctx);
    cursor = createCursor(operator, rightValue, ctx);
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

  private Object[] toBetweenIndexKey(Index definition, Object rightValue) {
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

  private IndexCursor createCursor(final BinaryCompareOperator operator, Object value, final CommandContext ctx) {
    // TODO: WHAT TO DO WITH ASCORDER?

    final Object[] values;
    if (!(value instanceof Object[]))
      values = new Object[] { value };
    else
      values = (Object[]) value;

    if (operator instanceof EqualsCompareOperator) {
      //return index.get(values);
      return index.range(orderAsc, values, true, values, true);
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

  private PCollection indexKeyFrom(final AndBlock keyCondition, final BinaryCondition additional) {
    final PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        final BinaryCondition binaryCond = ((BinaryCondition) exp);
        final BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof GtOperator) || (operator instanceof GeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else if (exp instanceof InCondition) {
        final Expression item = new Expression(-1);
        if (((InCondition) exp).getRightMathExpression() != null) {
          item.setMathExpression(((InCondition) exp).getRightMathExpression());
          result.add(item);
        } else if (((InCondition) exp).getRightParam() != null) {
          BaseExpression e = new BaseExpression(-1);
          e.setInputParam(((InCondition) exp).getRightParam().copy());
          item.setMathExpression(e);
          result.add(item);
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else if (exp instanceof ContainsAnyCondition) {
        if (((ContainsAnyCondition) exp).getRight() != null) {
          result.add(((ContainsAnyCondition) exp).getRight());
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private PCollection indexKeyTo(final AndBlock keyCondition, final BinaryCondition additional) {
    final PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        final BinaryCondition binaryCond = ((BinaryCondition) exp);
        final BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof LtOperator) || (operator instanceof LeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else if (exp instanceof InCondition) {
        final Expression item = new Expression(-1);
        if (((InCondition) exp).getRightMathExpression() != null) {
          item.setMathExpression(((InCondition) exp).getRightMathExpression());
          result.add(item);
        } else if (((InCondition) exp).getRightParam() != null) {
          BaseExpression e = new BaseExpression(-1);
          e.setInputParam(((InCondition) exp).getRightParam().copy());
          item.setMathExpression(e);
          result.add(item);
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else if (exp instanceof ContainsAnyCondition) {
        if (((ContainsAnyCondition) exp).getRight() != null) {
          result.add(((ContainsAnyCondition) exp).getRight());
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(final AndBlock keyCondition, final BinaryCondition additional) {
    final BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    final BinaryCompareOperator additionalOperator = additional == null ? null : additional.getOperator();
    if (exp instanceof BinaryCondition) {
      final BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      if (isGreaterOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
    } else if (exp instanceof InCondition || exp instanceof ContainsAnyCondition) {
      return additional == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
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
    final BinaryCompareOperator additionalOperator = additional == null ? null : additional.getOperator();
    if (exp instanceof BinaryCondition) {
      final BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      if (isLessOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else if (exp instanceof InCondition || exp instanceof ContainsAnyCondition) {
      return additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX " + indexName;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (condition != null) {
      result += ("\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additionalRangeCondition == null ?
          "" :
          " and " + additionalRangeCondition));
    }

    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public Result serialize() {
    final ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    result.setProperty("indexName", index.getName());
    if (condition != null) {
      result.setProperty("condition", condition.serialize());
    }
    if (additionalRangeCondition != null) {
      result.setProperty("additionalRangeCondition", additionalRangeCondition.serialize());
    }
    result.setProperty("orderAsc", orderAsc);
    return result;
  }

  @Override
  public void deserialize(final Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      indexName = fromResult.getProperty("indexName");
      if (fromResult.getProperty("condition") != null) {
        condition = BooleanExpression.deserializeFromOResult(fromResult.getProperty("condition"));
      }
      if (fromResult.getProperty("additionalRangeCondition") != null) {
        additionalRangeCondition = new BinaryCondition(-1);
        additionalRangeCondition.deserialize(fromResult.getProperty("additionalRangeCondition"));
      }
      orderAsc = fromResult.getProperty("orderAsc");
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
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
  public ExecutionStep copy(final CommandContext ctx) {
    return new FetchFromIndexStep(index, this.condition == null ? null : this.condition.copy(),
        this.additionalRangeCondition == null ? null : this.additionalRangeCondition.copy(), this.orderAsc, ctx, this.profilingEnabled);
  }
}
