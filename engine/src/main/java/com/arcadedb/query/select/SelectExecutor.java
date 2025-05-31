package com.arcadedb.query.select;/*
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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectExecutor {
  final Select select;
  long            evaluatedRecords = 0;
  List<IndexInfo> usedIndexes      = null;

  static class IndexInfo {
    public final Index   index;
    public final String  property;
    public final boolean order;

    IndexInfo(final Index index, final String property, final boolean order) {
      this.index = index;
      this.property = property;
      this.order = order;
    }
  }

  public SelectExecutor(final Select select) {
    this.select = select;
  }

  <T extends Document> SelectIterator<T> execute() {
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();

    final Iterator<? extends Identifiable> iterator;

    if (iteratorFromIndexes != null)
      iterator = iteratorFromIndexes;
    else if (select.fromType != null)
      iterator = select.database.iterateType(select.fromType.getName(), select.polymorphic);
    else if (select.fromBuckets.size() == 1)
      iterator = select.database.iterateBucket(select.fromBuckets.get(0).getName());
    else {
      final MultiIterator<? extends Identifiable> multiIterator = new MultiIterator<>();
      for (Bucket b : select.fromBuckets)
        multiIterator.addIterator(b.iterator());
      iterator = multiIterator;
    }

    if (select.timeoutInMs > 0 && iterator instanceof MultiIterator<? extends Identifiable> multiIterator)
      multiIterator.setTimeout(select.timeoutInMs, select.exceptionOnTimeout);

    if (select.parallel)
      return new SelectParallelIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);

    return new SelectIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);
  }

  private MultiIndexCursor lookForIndexes() {
    if (select.fromType != null && select.rootTreeElement != null) {
      final List<IndexCursor> cursors = new ArrayList<>();

      // FIND AVAILABLE INDEXES
      boolean canUseIndexes = isTheNodeFullyIndexed(select.rootTreeElement);

      filterWithIndexes(select.rootTreeElement, cursors);
      if (!cursors.isEmpty())
        return new MultiIndexCursor(cursors, select.limit, true);
    }
    return null;
  }

  private void filterWithIndexes(final SelectTreeNode node, final List<IndexCursor> cursors) {
    if (!(node.left instanceof SelectTreeNode))
      filterWithIndexesFinalNode(node, cursors);
    else {
      filterWithIndexes((SelectTreeNode) node.left, cursors);
      if (node.right != null)
        filterWithIndexes((SelectTreeNode) node.right, cursors);
    }
  }

  private void filterWithIndexesFinalNode(final SelectTreeNode node, final List<IndexCursor> cursors) {
    if (node.index == null)
      return;

    if (node.getParent().operator == SelectOperator.or) {
      if (node != node.getParent().right &&//
          !isTheNodeFullyIndexed((SelectTreeNode) node.getParent().right))
        // UNDER AN 'OR' OPERATOR BUT THE OTHER RIGHT VALUE IS NOT INDEXED: CANNOT USE THE CURRENT INDEX
        return;
    }

    final Object rightValue;
    if (node.right instanceof SelectParameterValue value)
      rightValue = value.eval(null);
    else
      rightValue = node.right;

    final String propertyName = ((SelectPropertyValue) node.left).propertyName;

    boolean ascendingOrder = true; // DEFAULT

    final IndexCursor cursor;
    if (node.operator == SelectOperator.eq)
      cursor = node.index.get(new Object[] { rightValue });
    else {
      // RANGE
      if (select.orderBy != null)
        for (Pair<String, Boolean> entry : select.orderBy) {
          if (propertyName.equals(entry.getFirst())) {
            if (!entry.getSecond())
              ascendingOrder = false;
            break;
          }
        }

      if (node.operator == SelectOperator.gt)
        cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, false, null, false);
      else if (node.operator == SelectOperator.ge)
        cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, true, null, false);
      else if (node.operator == SelectOperator.lt)
        cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, false);
      else if (node.operator == SelectOperator.le)
        cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, true);
      else
        return;
    }

    final SelectTreeNode parentNode = node.getParent();
    if (parentNode.operator == SelectOperator.and && parentNode.left == node) {
      if (!node.index.isUnique()) {
        // CHECK IF THERE IS ANOTHER INDEXED NODE ON THE SIBLING THAT IS UNIQUE (TO PREFER TO THIS)
        final TypeIndex rightIndex = ((SelectTreeNode) parentNode.right).index;
        if (rightIndex != null && rightIndex.isUnique()) {
          // DO NOT USE THIS INDEX (NOT UNIQUE), NOT WORTH IT
          node.index = null;
          return;
        }
      } else {
        // REMOVE THE INDEX ON THE SIBLING NODE
        // TODO CALCULATE WHICH ONE IS FASTER AND REMOVE THE SLOWER ONE
        ((SelectTreeNode) parentNode.right).index = null;
      }
    }

    if (usedIndexes == null)
      usedIndexes = new ArrayList<>();

    usedIndexes.add(new IndexInfo(node.index, propertyName, ascendingOrder));
    cursors.add(cursor);
  }

  /**
   * Considers a fully indexed node when both properties are indexed or only one with an AND operator.
   */
  private boolean isTheNodeFullyIndexed(final SelectTreeNode node) {
    if (node == null)
      return true;

    if (!(node.left instanceof SelectTreeNode)) {
      if (!(node.right instanceof SelectPropertyValue)) {
        final TypeIndex propertyIndex = select.fromType.getPolymorphicIndexByProperties(
            ((SelectPropertyValue) node.left).propertyName);

        if (propertyIndex != null)
          node.index = propertyIndex;

        return propertyIndex != null;
      }
    } else {
      final boolean leftIsIndexed = isTheNodeFullyIndexed((SelectTreeNode) node.left);
      final boolean rightIsIndexed = isTheNodeFullyIndexed((SelectTreeNode) node.right);

      if (node.operator.equals(SelectOperator.and))
        // AND: ONE OR BOTH MEANS INDEXED
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(SelectOperator.or))
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(SelectOperator.not))
        return leftIsIndexed;
    }
    return false;
  }

  public static Object evaluateValue(final Document record, final Object value) {
    if (value == null)
      return null;
    else if (value instanceof SelectTreeNode node)
      return node.eval(record);
    else if (value instanceof SelectRuntimeValue runtimeValue)
      return runtimeValue.eval(record);
    return value;
  }

  public Map<String, Object> metrics() {
    return Map.of("evaluatedRecords", evaluatedRecords, "usedIndexes", usedIndexes != null ? usedIndexes.size() : 0);
  }

  boolean evaluateWhere(final Document record) {
    ++evaluatedRecords;
    final Object result = select.rootTreeElement.eval(record);
    if (result instanceof Boolean boolean1)
      return boolean1;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }
}
