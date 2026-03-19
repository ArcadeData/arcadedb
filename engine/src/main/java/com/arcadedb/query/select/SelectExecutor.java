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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.*;

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
    final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);

    if (select.parallel)
      return new SelectParallelIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);

    return new SelectIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);
  }

  long executeCount() {
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();
    final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);
    final Set<RID> filterOutRecords = (iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1)
        ? ConcurrentHashMap.newKeySet() : null;

    long count = 0;
    int skipped = 0;
    while (iterator.hasNext()) {
      final Document record = iterator.next().asDocument();
      if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
        continue;
      if (select.rootTreeElement == null || evaluateWhere(record)) {
        if (skipped < select.skip) {
          skipped++;
          continue;
        }
        if (filterOutRecords != null)
          filterOutRecords.add(record.getIdentity());
        count++;
        if (select.limit > -1 && count >= select.limit)
          break;
      }
    }
    return count;
  }

  boolean executeExists() {
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();
    final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);

    while (iterator.hasNext()) {
      final Document record = iterator.next().asDocument();
      if (select.rootTreeElement == null || evaluateWhere(record))
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  <T extends Document> List<SelectVectorResult<T>> executeVector() {
    if (select.fromType == null)
      throw new IllegalArgumentException("FromType must be set for vector search");
    if (select.vectorProperty == null)
      throw new IllegalArgumentException("Vector property must be set (call nearestTo())");

    final TypeIndex typeIndex = select.fromType.getPolymorphicIndexByProperties(select.vectorProperty);
    if (typeIndex == null)
      throw new IllegalArgumentException("No index found on property '" + select.vectorProperty + "' for type '" + select.fromType.getName() + "'");

    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0)
      throw new IllegalArgumentException("Index '" + typeIndex.getName() + "' has no bucket indexes");

    final List<LSMVectorIndex> vectorIndexes = new ArrayList<>();
    for (final IndexInternal bucketIndex : bucketIndexes)
      if (bucketIndex instanceof LSMVectorIndex lsmIndex)
        vectorIndexes.add(lsmIndex);

    if (vectorIndexes.isEmpty())
      throw new IllegalArgumentException("Index '" + typeIndex.getName() + "' is not a vector index");

    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();
    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      final List<Pair<RID, Float>> neighbors;
      if (select.vectorApproximate)
        neighbors = lsmIndex.findNeighborsFromVectorApproximate(select.vectorQuery, select.vectorK);
      else
        neighbors = lsmIndex.findNeighborsFromVector(select.vectorQuery, select.vectorK);
      allNeighbors.addAll(neighbors);
    }

    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    final int resultCount = Math.min(select.vectorK, allNeighbors.size());
    final List<SelectVectorResult<T>> results = new ArrayList<>(resultCount);

    for (int i = 0; i < resultCount; i++) {
      final Pair<RID, Float> neighbor = allNeighbors.get(i);
      final T record = (T) neighbor.getFirst().asDocument();

      if (select.rootTreeElement != null && !evaluateWhere(record))
        continue;

      results.add(new SelectVectorResult<>(record, neighbor.getSecond()));
    }

    return results;
  }

  private Iterator<? extends Identifiable> buildIterator(final MultiIndexCursor iteratorFromIndexes) {
    final Iterator<? extends Identifiable> iterator;

    if (iteratorFromIndexes != null)
      iterator = iteratorFromIndexes;
    else if (select.fromType != null)
      iterator = select.database.iterateType(select.fromType.getName(), select.polymorphic);
    else if (select.fromBuckets.size() == 1)
      iterator = select.database.iterateBucket(select.fromBuckets.getFirst().getName());
    else {
      final MultiIterator<? extends Identifiable> multiIterator = new MultiIterator<>();
      for (Bucket b : select.fromBuckets)
        multiIterator.addIterator(b.iterator());
      iterator = multiIterator;
    }

    if (select.timeoutInMs > 0 && iterator instanceof MultiIterator<? extends Identifiable> multiIterator)
      multiIterator.setTimeout(select.timeoutInMs, select.exceptionOnTimeout);

    return iterator;
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
      // UNDER AN 'OR' OPERATOR: BOTH SIDES MUST BE INDEXED, OTHERWISE CANNOT USE INDEXES
      if (node != node.getParent().right) {
        if (!isTheNodeFullyIndexed((SelectTreeNode) node.getParent().right))
          return;
      } else {
        if (node.getParent().left instanceof SelectTreeNode leftNode && !isTheNodeFullyIndexed(leftNode))
          return;
      }
    }

    final Object rightValue;
    if (node.right instanceof SelectParameterValue value)
      rightValue = value.eval(null);
    else
      rightValue = node.right;

    final String propertyName = ((SelectPropertyValue) node.left).propertyName;

    boolean ascendingOrder = true; // DEFAULT

    if (select.orderBy != null)
      for (Pair<String, Boolean> entry : select.orderBy) {
        if (propertyName.equals(entry.getFirst())) {
          if (!entry.getSecond())
            ascendingOrder = false;
          break;
        }
      }

    final IndexCursor cursor;
    if (node.operator == SelectOperator.eq)
      cursor = node.index.get(new Object[] { rightValue });
    else if (node.operator == SelectOperator.in_op) {
      // IN: multi-point index lookup
      if (rightValue instanceof Collection<?> collection) {
        final List<IndexCursor> inCursors = new ArrayList<>();
        for (final Object item : collection)
          inCursors.add(node.index.get(new Object[] { item }));
        cursor = inCursors.isEmpty() ? null : new MultiIndexCursor(inCursors, select.limit, ascendingOrder);
      } else
        cursor = node.index.get(new Object[] { rightValue });
    } else if (node.operator == SelectOperator.between) {
      // BETWEEN: range scan
      if (rightValue instanceof Object[] range && range.length == 2)
        cursor = node.index.range(ascendingOrder, new Object[] { range[0] }, true, new Object[] { range[1] }, true);
      else
        return;
    } else if (node.operator == SelectOperator.gt)
      cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, false, null, false);
    else if (node.operator == SelectOperator.ge)
      cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, true, null, false);
    else if (node.operator == SelectOperator.lt)
      cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, false);
    else if (node.operator == SelectOperator.le)
      cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, true);
    else
      return;

    if (cursor == null)
      return;

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
      if (node.operator == SelectOperator.is_null || node.operator == SelectOperator.is_not_null)
        return false;

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
