package com.arcadedb.query.nativ;/*
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
 */

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.MultiIterator;

import java.util.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelectExecutor {
  final   NativeSelect select;
  private long         evaluatedRecords = 0;
  private int          indexesUsed      = 0;

  public NativeSelectExecutor(final NativeSelect select) {
    this.select = select;
  }

  <T extends Identifiable> QueryIterator<T> execute() {
    final Iterator<Identifiable> iteratorFromIndexes = lookForIndexes();

    final Iterator<Identifiable> iterator;

    if (iteratorFromIndexes != null)
      iterator = iteratorFromIndexes;
    else if (select.fromType != null)
      iterator = (MultiIterator) select.database.iterateType(select.fromType.getName(), select.polymorphic);
    else if (select.fromBuckets.size() == 1)
      iterator = (MultiIterator) select.database.iterateBucket(select.fromBuckets.get(0).getName());
    else {
      final MultiIterator multiIterator = new MultiIterator<>();
      for (Bucket b : select.fromBuckets)
        multiIterator.addIterator(b.iterator());
      iterator = multiIterator;
    }

    if (select.timeoutUnit != null && iterator instanceof MultiIterator)
      ((MultiIterator<Identifiable>) iterator).setTimeout(select.timeoutUnit.toMillis(select.timeoutValue),
          select.exceptionOnTimeout);

    return new QueryIterator<>(this, iterator, iteratorFromIndexes != null);
  }

  private Iterator<Identifiable> lookForIndexes() {
    if (select.fromType != null && select.rootTreeElement != null) {
      final List<IndexCursor> cursors = new ArrayList<>();

      // FIND AVAILABLE INDEXES
      boolean canUseIndexes = isTheNodeFullyIndexed(select.rootTreeElement);

      filterWithIndexes(select.rootTreeElement, cursors);
      if (!cursors.isEmpty()) {
        indexesUsed = cursors.size();
        return new MultiIndexCursor(cursors, select.limit, true);
      }
    }
    return null;
  }

  private void filterWithIndexes(final NativeTreeNode node, final List<IndexCursor> cursors) {
    if (!(node.left instanceof NativeTreeNode))
      filterWithIndexesFinalNode(node, cursors);
    else {
      filterWithIndexes((NativeTreeNode) node.left, cursors);
      if (node.right != null)
        filterWithIndexes((NativeTreeNode) node.right, cursors);
    }
  }

  private void filterWithIndexesFinalNode(final NativeTreeNode node, final List<IndexCursor> cursors) {
    if (node.index == null)
      return;

    if (node.getParent().operator == NativeOperator.or) {
      if (node != node.getParent().right &&//
          !isTheNodeFullyIndexed((NativeTreeNode) node.getParent().right))
        // UNDER AN 'OR' OPERATOR BUT THE OTHER RIGHT VALUE IS NOT INDEXED: CANNOT USE THE CURRENT INDEX
        return;
    }

    final Object rightValue;
    if (node.right instanceof NativeParameterValue)
      rightValue = ((NativeParameterValue) node.right).eval(null);
    else
      rightValue = node.right;

    final IndexCursor cursor;
    if (node.operator == NativeOperator.eq)
      cursor = node.index.get(new Object[] { rightValue });
    else if (node.operator == NativeOperator.gt)
      cursor = node.index.range(true, new Object[] { rightValue }, false, null, false);
    else if (node.operator == NativeOperator.ge)
      cursor = node.index.range(true, new Object[] { rightValue }, true, null, false);
    else if (node.operator == NativeOperator.lt)
      cursor = node.index.range(true, null, false, new Object[] { rightValue }, false);
    else if (node.operator == NativeOperator.le)
      cursor = node.index.range(true, null, false, new Object[] { rightValue }, true);
    else
      return;

    final NativeTreeNode parentNode = node.getParent();
    if (parentNode.operator == NativeOperator.and && parentNode.left == node) {
      if (!node.index.isUnique()) {
        // CHECK IF THERE IS ANOTHER INDEXED NODE ON THE SIBLING THAT IS UNIQUE (TO PREFER TO THIS)
        final TypeIndex rightIndex = ((NativeTreeNode) parentNode.right).index;
        if (rightIndex != null && rightIndex.isUnique()) {
          // DO NOT USE THIS INDEX (NOT UNIQUE), NOT WORTH IT
          node.index = null;
          return;
        }
      } else {
        // REMOVE THE INDEX ON THE SIBLING NODE
        // TODO CALCULATE WHICH ONE IS FASTER AND REMOVE THE SLOWER ONE
        ((NativeTreeNode) parentNode.right).index = null;
      }
    }

    cursors.add(cursor);
  }

  /**
   * Considers a fully indexed node when both properties are indexed or only one with an AND operator.
   */
  private boolean isTheNodeFullyIndexed(final NativeTreeNode node) {
    if (node == null)
      return true;

    if (!(node.left instanceof NativeTreeNode)) {
      if (!(node.right instanceof NativePropertyValue)) {
        final TypeIndex propertyIndex = select.fromType.getPolymorphicIndexByProperties(
            ((NativePropertyValue) node.left).propertyName);

        if (propertyIndex != null)
          node.index = propertyIndex;

        return propertyIndex != null;
      }
    } else {
      final boolean leftIsIndexed = isTheNodeFullyIndexed((NativeTreeNode) node.left);
      final boolean rightIsIndexed = isTheNodeFullyIndexed((NativeTreeNode) node.right);

      if (node.operator.equals(NativeOperator.and))
        // AND: ONE OR BOTH MEANS INDEXED
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(NativeOperator.or))
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(NativeOperator.not))
        return leftIsIndexed;
    }
    return false;
  }

  public static Object evaluateValue(final Document record, final Object value) {
    if (value == null)
      return null;
    else if (value instanceof NativeTreeNode)
      return ((NativeTreeNode) value).eval(record);
    else if (value instanceof NativeRuntimeValue)
      return ((NativeRuntimeValue) value).eval(record);
    return value;
  }

  public Map<String, Object> metrics() {
    return Map.of("evaluatedRecords", evaluatedRecords, "indexesUsed", indexesUsed);
  }

  boolean evaluateWhere(final Document record) {
    ++evaluatedRecords;
    final Object result = select.rootTreeElement.eval(record);
    if (result instanceof Boolean)
      return (Boolean) result;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }
}
