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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.ContainsTextCondition;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.IsNullCondition;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.PCollection;
import com.arcadedb.query.sql.parser.ValueExpression;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Created by luigidellaquila on 23/07/16.
 */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected final String                                         indexName;
  /** Package-private so a test can observe that a restart released them instead of only dropping the references. */
  final           List<IndexCursor>                              nextCursors = new ArrayList<>();
  /**
   * The per-value cursors an {@code IN} condition opens ({@link #processInCondition()} hands each to
   * {@code customIterator} rather than to {@code nextCursors}). Held here only so {@link #releaseCursors()} can close
   * them: nothing else ever would, so an {@code IN} list left the same retired-file guard behind that {@code close()}
   * exists to release.
   */
  final           List<IndexCursor>                              customCursors = new ArrayList<>();
  protected       RangeIndex                                     index;
  protected       BooleanExpression                              condition;
  private       BinaryCondition additionalRangeCondition;
  private final boolean         orderAsc;
  private       long            count       = 0;
  private         boolean                                        inited      = false;
  /** Package-private for the same reason as {@link #nextCursors}. */
  IndexCursor                                                    cursor;
  private         MultiIterator<Map.Entry<Object, Identifiable>> customIterator;
  private         Pair<Object, Identifiable>                     nextEntry   = null;
  // Float so full-text BM25 scores below 1.0 are not truncated to 0 (which the `> 0` guard would then suppress). For
  // integer-scored indexes getFloatScore() returns the int value unchanged.
  private         float                                          nextEntryScore = 0f;

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
    // Pull previous steps first so that GlobalLetQueryStep (if present) can populate
    // context variables (e.g. subquery results) before init() evaluates the condition.
    pullPrevious(context, nRecords);

    init(context.getDatabase());

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
          if (nextEntryScore > 0f)
            result.setProperty("$score", nextEntryScore);
          context.setVariable("current", result);
          return result;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }
    };
  }

  private void fetchNextEntry() {
    nextEntry = null;
    // Defensive loop guard: each iteration either returns, drops one element from
    // nextCursors, or toggles `cursor` between null and the next pending entry.
    // The bound is therefore proportional to the initial nextCursors size; a small
    // generous multiplier protects against any future cursor implementation that
    // misbehaves without depending on the underlying LSM/MultiIndex guards.
    int safetyCounter = 0;
    final int safetyLimit = (nextCursors.size() + 2) * 4;
    while (true) {
      if (++safetyCounter > safetyLimit)
        throw new IllegalStateException(
            "Detected infinite loop while fetching from index '" + indexName + "' (iterations=" + safetyCounter
                + "). The index may be corrupted, please rebuild it.");

      if (cursor == null) {
        if (nextCursors.isEmpty()) {
          if (nextEntry == null && customIterator != null && customIterator.hasNext()) {
            final Map.Entry<Object, Identifiable> entry = customIterator.next();
            nextEntry = new Pair<>(entry.getKey(), entry.getValue().getIdentity());
            nextEntryScore = 0;
          }

          if (nextEntry == null)
            updateIndexStats();
          else
            count++;

          return;
        }
        cursor = nextCursors.remove(0);
      }
      if (cursor.hasNext()) {
        final Object value = cursor.next();
        nextEntry = new Pair(cursor.getKeys(), value);
        nextEntryScore = cursor.getFloatScore();
        count++;
        return;
      }

      cursor = null;
    }
  }

  @Override
  public void close() {
    releaseCursors();
    super.close();
  }

  /**
   * Releases the index cursors even when the scan did not run to exhaustion (e.g. a LIMIT was reached, the result set
   * was closed early, or the step is being restarted): compacted-series cursors register with their file so a full
   * compaction defers dropping it, and an unclosed cursor would keep the retired file alive until the next database
   * restart.
   */
  private void releaseCursors() {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    for (final IndexCursor c : nextCursors)
      c.close();
    nextCursors.clear();
    for (final IndexCursor c : customCursors)
      c.close();
    customCursors.clear();
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
      } else if (lastOp instanceof BetweenCondition) {
        range = true;
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
        cost += System.nanoTime() - begin;
      }
    }
  }

  private void processInCondition() {
    final InCondition inCondition = (InCondition) condition;

    final Expression left = inCondition.getLeft();
    if (!"key".equalsIgnoreCase(left.toString())) {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    final Object rightValue = inCondition.evaluateRight((Result) null, context);
    final EqualsCompareOperator equals = new EqualsCompareOperator();
    if (MultiValue.isMultiValue(rightValue)) {
      customIterator = new MultiIterator<>();
      for (final Object item : MultiValue.getMultiValueIterable(rightValue)) {
        final IndexCursor localCursor = createCursor(equals, unwrapSubQueryResult(item), context);
        if (localCursor == null)
          // This IN-list item has no defined ordering against the index's declared key type (e.g. a non-numeric
          // String item against a numeric column): it matches no indexed row, consistent with the other operators
          // (#5900). Skip it rather than adding a cursor-less sub-iterator that NPEs on the first hasNext()/close().
          continue;
        customCursors.add(localCursor);

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
      cursor = createCursor(equals, unwrapSubQueryResult(rightValue), context);
    }
    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    if (processFullTextBlock())
      return;

    final PCollection fromKey = indexKeyFrom((AndBlock) condition, additionalRangeCondition);
    final PCollection toKey = indexKeyTo((AndBlock) condition, additionalRangeCondition);
    final boolean fromKeyIncluded = indexKeyFromIncluded((AndBlock) condition, additionalRangeCondition);
    final boolean toKeyIncluded = indexKeyToIncluded((AndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  /**
   * Builds the POSITIONAL key a full-text index expects and opens the lookup with it: {@code keys[i]} is the text to find in
   * the i-th indexed property, {@code null} leaving that property unconstrained. Field names matter here, unlike everywhere
   * else on this step - a full-text index key is one query string per property rather than a composite ordered key, so a
   * condition on the second property alone is a perfectly good key and must not be shifted into the first slot.
   * <p>
   * Answers false, leaving the generic path to run, whenever the block is not exactly that: another index type, a range
   * condition alongside, anything but {@code CONTAINSTEXT}, a property this index does not cover, or a multi-value key. A
   * single-property index takes this path too and produces the same one-element key it always did (issue #6414, item 2).
   * <p>
   * A property's slot can be filled more than once - two (or more) {@code CONTAINSTEXT} conditions on the same property,
   * which the planner now claims all of, or one document property indexed twice under different modifiers
   * ({@code (m by key, m by value)}). Rather than keeping one value, the slot then holds a {@code List} of them, and
   * {@link com.arcadedb.index.fulltext.LSMTreeFullTextIndex#splitPositionalKey(java.util.List, Object[])} expands each
   * list slot into one lookup per element, which {@link com.arcadedb.index.fulltext.LSMTreeFullTextIndex#get(Object[],
   * int)} intersects exactly as it already intersects one lookup per property (issue #6427).
   * <p>
   * A condition whose right side evaluates to {@code null} makes the whole block match NOTHING rather than leaving its
   * property unconstrained. The two readings share one slot - a {@code null} slot is exactly how the key says "this property
   * is not constrained" - so folding a null-valued condition into it would silently drop the condition instead of failing
   * it. That is what {@link ContainsTextCondition#evaluate} does off the index (a {@code null} value is never a match),
   * what a single-property index already did (an empty query text finds no term), and now what a multi-property one
   * does too.
   * <p>
   * The position-resolution loop below matches a condition's field name to an index property by its BASE name only, so
   * two index properties that share one base name but differ in modifier ({@code m by key} and {@code m by value})
   * are indistinguishable to it - a condition on {@code m} always resolves to whichever of the two comes FIRST in
   * {@code properties}. This was already true for a single such condition before issue #6427 (it silently searched only
   * the first modifier's tokens, never the second); what changes here is that a SECOND {@code m} condition, which used
   * to make the whole block bail to the generic path, now lands in the same slot as the first and is intersected
   * against it. Both conditions therefore query the same one modifier property - the other is never consulted - rather
   * than one condition per modifier. Resolving that would need the position lookup to track which specific index
   * property the planner associated each claimed condition with, rather than re-deriving it from the field name alone;
   * that is a distinct, narrower problem than #6427, whose fix is deliberately confined to the same-property,
   * same-modifier case the issue describes.
   */
  private boolean processFullTextBlock() {
    if (index.getType() != Schema.INDEX_TYPE.FULL_TEXT || additionalRangeCondition != null)
      return false;

    final List<String> properties = index.getPropertyNames();
    if (properties == null || properties.isEmpty())
      return false;

    final List<BooleanExpression> subBlocks = ((AndBlock) condition).getSubBlocks();
    if (subBlocks.isEmpty())
      return false;

    final Object[] keys = new Object[properties.size()];
    for (final BooleanExpression exp : subBlocks) {
      if (!(exp instanceof ContainsTextCondition textCondition))
        return false;

      final String fieldName = textCondition.getLeft().getDefaultAlias().getStringValue();
      int position = -1;
      for (int i = 0; i < properties.size(); i++)
        if (Index.basePropertyName(properties.get(i)).equals(fieldName)) {
          position = i;
          break;
        }

      if (position < 0)
        return false;

      final Object value = textCondition.getRight().execute((Result) null, context);
      if (value == null) {
        // Unsatisfiable, not unconstrained: no cursor at all, which fetchNextEntry() reads as an exhausted scan.
        cursor = null;
        fetchNextEntry();
        return true;
      }
      if (!(value instanceof Identifiable) && MultiValue.isMultiValue(value))
        return false;

      final Object existing = keys[position];
      if (existing == null) {
        keys[position] = value;
      } else if (existing instanceof List<?> existingList) {
        // Safe: the only producer of this list is the `else` branch below, freshly allocated as List<Object>. The
        // suppression is scoped to this one cast rather than the whole method.
        @SuppressWarnings("unchecked")
        final List<Object> objectList = (List<Object>) existingList;
        objectList.add(value);
      } else {
        final List<Object> values = new ArrayList<>(2);
        values.add(existing);
        values.add(value);
        keys[position] = values;
      }
    }

    cursor = index.get(keys);
    fetchNextEntry();
    return true;
  }

  private void processIsNullCondition() {
    final int keyCount = index.getPropertyNames().size();
    final Object[] nullKeys = new Object[keyCount];
    cursor = index.get(nullKeys);
    fetchNextEntry();
  }

  /**
   * A flat scan needs NO separate pass over the NULL-keyed entries, under any {@code NULL_STRATEGY}: with
   * {@code INDEX} they are already in the B-tree and the cursor returns them at their sorted position (first
   * ascending, last descending); with {@code SKIP} they were never indexed; with {@code ERROR} they could not be
   * inserted. A second iterator over {@code index.get(new Object[keyCount])} would therefore either duplicate rows
   * or return nothing - which is why the {@code fetchNullKeys()} that built one was never called and has been
   * removed (#5662).
   */
  private void processFlatIteration() {
    cursor = index.iterator(isOrderAsc());

    if (cursor != null) {
      fetchNextEntry();
    }
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

      if (!valuesConvertToIndexKeyTypes(convertedFrom) || !valuesConvertToIndexKeyTypes(convertedTo))
        // This combination's bound has no defined ordering against the index's declared key type: it matches no
        // indexed row, consistent with the row-scan operators (#5900). Skip it rather than aborting the whole scan.
        continue;

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
      cursor = nextCursors.remove(0);
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
    return cartesianProduct(new PCollection(), key);//TODO
  }

  private List<PCollection> cartesianProduct(final PCollection head, final PCollection key) {
    if (key.getExpressions().isEmpty())
      return List.of(head);

    final Expression nextElementInKey = key.getExpressions().get(0);
    final Object value = nextElementInKey.execute(new ResultInternal(context.getDatabase()), context);
    // A multi-value key expands into one index lookup per element. MultiValue covers every shape a
    // parameter can take, including primitive arrays (long[]/int[]/double[]) that are not Iterable,
    // consistent with the multi-value handling in processInCondition().
    if (!(value instanceof Identifiable) && MultiValue.isMultiValue(value)) {
      final List<PCollection> result = new ArrayList<>();
      for (final Object elemInKey : MultiValue.getMultiValueIterable(value)) {
        final PCollection newHead = new PCollection();
        for (final Expression exp : head.getExpressions())
          newHead.add(exp.copy());

        newHead.add(toExpression(unwrapSubQueryResult(elemInKey)));
        final PCollection tail = key.copy();
        tail.getExpressions().remove(0);
        result.addAll(cartesianProduct(newHead, tail));
      }
      return result;
    } else {
      final PCollection newHead = new PCollection();
      for (final Expression exp : head.getExpressions())
        newHead.add(exp.copy());

      newHead.add(nextElementInKey);
      final PCollection tail = key.copy();
      tail.getExpressions().remove(0);
      return cartesianProduct(newHead, tail);
    }

  }

  private Expression toExpression(final Object value) {
    return new ValueExpression(value);
  }

  /**
   * Unwraps a Result object from a subquery when it has exactly one property.
   * This is needed when IN (SELECT ...) results are used as index keys: the index stores
   * the raw property value (e.g. "hello"), but the subquery returns Result objects like
   * {name: "hello"}. Without unwrapping, the index lookup would fail to find any match.
   */
  private static Object unwrapSubQueryResult(final Object item) {
    if (item instanceof Result result && !result.isElement()) {
      final Set<String> propertyNames = result.getPropertyNames();
      if (propertyNames.size() == 1)
        return result.getProperty(propertyNames.iterator().next());
    }
    return item;
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
    if (!"key".equalsIgnoreCase(key.toString()))
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");

    final Expression second = ((BetweenCondition) condition).getSecond();
    final Expression third = ((BetweenCondition) condition).getThird();

    final Object secondValue = second.execute((Result) null, context);
    final Object thirdValue = third.execute((Result) null, context);

    if (!valuesConvertToIndexKeyTypes(new Object[] { secondValue }) || !valuesConvertToIndexKeyTypes(new Object[] { thirdValue }))
      // A bound has no defined ordering against the index's declared key type: no indexed row can match (#5900).
      cursor = null;
    else if (isOrderAsc())
      cursor = index.range(true, new Object[] { secondValue }, true, new Object[] { thirdValue }, true);
    else
      cursor = index.range(false, new Object[] { thirdValue }, true, new Object[] { secondValue }, true);

    if (cursor != null)
      fetchNextEntry();
  }

  private void processBinaryCondition() {
    final BinaryCompareOperator operator = ((BinaryCondition) condition).getOperator();
    final Expression left = ((BinaryCondition) condition).getLeft();
    if (!"key".equalsIgnoreCase(left.toString())) {
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

    if (!valuesConvertToIndexKeyTypes(values))
      // A bound has no defined ordering against the index's declared key type (e.g. a non-numeric String bound on
      // a numeric column): no indexed row can match, consistent with the row-scan operators (#5900).
      return null;

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

  /**
   * Best-effort check that every non-null value converts to the index's declared key type, mirroring exactly what
   * the index's own key conversion does internally ({@code Type.convert} against
   * {@code BinaryTypes.getClassFromType(...)}, e.g. {@code LSMTreeIndexAbstract.convertKeys()}). Run BEFORE calling
   * into the index so a genuine type mismatch is what gets treated as "no match" here - catching whatever
   * {@code IllegalArgumentException} the index call happens to throw would also silently swallow unrelated causes
   * (a NULL-strategy violation, a non-range-capable composite sub-index) as the same "no rows" outcome.
   */
  private boolean valuesConvertToIndexKeyTypes(final Object[] values) {
    if (values == null || !(index instanceof IndexInternal internalIndex))
      return true;

    final byte[] keyTypes = internalIndex.getBinaryKeyTypes();
    final Database database = context.getDatabase();
    // Bounded by the shorter array rather than requiring equal lengths: convertKeys() itself assumes
    // values.length <= keyTypes.length (it indexes keyTypes[i] unguarded for every key), so a partial-key
    // lookup is expected to supply no more values than the index has key types for.
    for (int i = 0; i < values.length && i < keyTypes.length; i++) {
      if (values[i] == null)
        continue;
      try {
        Type.convert(database, values[i], BinaryTypes.getClassFromType(keyTypes[i]));
      } catch (final IllegalArgumentException e) {
        return false;
      }
    }
    return true;
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private static PCollection indexKeyFrom(final AndBlock keyCondition, final BinaryCondition additional) {
    PCollection result = new PCollection();
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      Expression res = exp.resolveKeyFrom(additional);
      if (res != null) {
        result.add(res);
      }
    }
    return result;
  }

  private static PCollection indexKeyTo(final AndBlock keyCondition, final BinaryCondition additional) {
    PCollection result = new PCollection();
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
      result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additionalRangeCondition == null ?
          "" :
          " and " + additionalRangeCondition);
    }

    return result;
  }

  /**
   * Restarts the step: {@code inited} goes back to false and {@code init()} rebuilds the whole cursor set from scratch
   * (that is what {@code UpdateExecutionPlan.reset()} relies on - it re-runs the plan straight after). So the cursors of
   * the previous run must be RELEASED here, not just dropped (#5635):
   * <ul>
   *   <li>a {@code LSMTreeIndexUnderlyingCompactedSeriesCursor} stays registered with its file, and
   *       {@code LSMTreeIndex.dropRetiredCompactedIndexes} skips a retired file that still has one - for the lifetime of
   *       the database, since nothing else will ever close it;</li>
   *   <li>{@code nextCursors} was not even cleared, so the pending cursors of the previous run survived into the new one
   *       and {@code init()} appended to them: the restarted scan replayed the OLD, partly consumed cursors before
   *       reaching the ones it had just opened.</li>
   * </ul>
   * Not propagated to {@code prev}: {@code SelectExecutionPlan.reset()} walks every step itself, so a step that reset
   * its predecessor would reset it twice.
   */
  @Override
  public void reset() {
    releaseCursors();

    index = null;
    condition = condition == null ? null : condition.copy();
    additionalRangeCondition = additionalRangeCondition == null ? null : additionalRangeCondition.copy();

    cost = 0;
    count = 0;

    inited = false;
    customIterator = null;
    nextEntry = null;
    nextEntryScore = 0f;
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
