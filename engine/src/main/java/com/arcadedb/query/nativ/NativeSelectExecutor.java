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
import org.eclipse.collections.impl.lazy.parallel.set.sorted.SelectSortedSetBatch;

import java.util.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelectExecutor {
  private final NativeSelect select;
  private       long         browsed     = 0;
  private       int          indexesUsed = 0;

  public NativeSelectExecutor(final NativeSelect select) {
    this.select = select;
  }

  <T extends Identifiable> QueryIterator<T> execute() {
    final int[] returned = new int[] { 0 };

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

    return new QueryIterator<>() {
      private T next = null;

      @Override
      public boolean hasNext() {
        if (select.limit > -1 && returned[0] >= select.limit)
          return false;
        if (next != null)
          return true;
        if (!iterator.hasNext())
          return false;

        next = fetchNext();
        return next != null;
      }

      @Override
      public T next() {
        if (next == null && !hasNext())
          throw new NoSuchElementException();
        try {
          return next;
        } finally {
          next = null;
        }
      }

      private T fetchNext() {
        do {
          final Document record = iterator.next().asDocument();
          if (evaluateWhere(record)) {
            ++returned[0];
            return (T) record;
          }

        } while (iterator.hasNext());

        // NOT FOUND
        return null;
      }
    };
  }

  private Iterator<Identifiable> lookForIndexes() {
    if (select.fromType != null && select.rootTreeElement != null) {
      final List<IndexCursor> indexes = new ArrayList<>();
      lookForIndexes(select.rootTreeElement, indexes);
      if (!indexes.isEmpty()) {
        indexesUsed = indexes.size();
        return new MultiIndexCursor(indexes, select.limit, true);
      }
    }
    return null;
  }

  private void lookForIndexes(final NativeTreeNode node, final List<IndexCursor> indexes) {
    if (!(node.left instanceof NativeTreeNode))
      lookForIndexesFinalNode(node, indexes);
    else {
      lookForIndexes((NativeTreeNode) node.left, indexes);
      if (node.right != null)
        lookForIndexes((NativeTreeNode) node.right, indexes);
    }
  }

  private void lookForIndexesFinalNode(final NativeTreeNode node, final List<IndexCursor> indexes) {
    if (node.left instanceof NativePropertyValue) {
      if (!(node.right instanceof NativePropertyValue)) {
        final List<TypeIndex> propertyIndexes = select.fromType.getIndexesByProperties(
            ((NativePropertyValue) node.left).propertyName);
        if (!propertyIndexes.isEmpty()) {
          if (node.getParent().operator == NativeOperator.or) {
            if (!isLeftPropertyIndexed((NativeTreeNode) node.getParent().right))
              // UNDER AN 'OR' OPERATOR BUT THE OTHER RIGHT VALUE IS NOT INDEXED: CANNOT USE THE CURRENT INDEX
              return;
          }

          final Object rightValue;
          if (node.right instanceof NativeParameterValue)
            rightValue = ((NativeParameterValue) node.right).eval(null);
          else
            rightValue = node.right;

          for (TypeIndex idx : propertyIndexes) {
            final IndexCursor cursor;
            if (node.operator == NativeOperator.eq)
              cursor = idx.get(new Object[] { rightValue });
            else if (node.operator == NativeOperator.gt)
              cursor = idx.range(true, new Object[] { rightValue }, false, null, false);
            else if (node.operator == NativeOperator.ge)
              cursor = idx.range(true, new Object[] { rightValue }, true, null, false);
            else if (node.operator == NativeOperator.lt)
              cursor = idx.range(true, null, false, new Object[] { rightValue }, false);
            else if (node.operator == NativeOperator.le)
              cursor = idx.range(true, null, false, new Object[] { rightValue }, true);
            else
              continue;

            indexes.add(cursor);
          }
        }
      }
    }
  }

  private boolean isLeftPropertyIndexed(final NativeTreeNode node) {
    if (node.left instanceof NativePropertyValue) {
      if (!(node.right instanceof NativePropertyValue)) {
        final List<TypeIndex> propertyIndexes = select.fromType.getIndexesByProperties(
            ((NativePropertyValue) node.left).propertyName);
        if (!propertyIndexes.isEmpty()) {
          return true;
        }
      }
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
    return Map.of("browsed", browsed, "indexesUsed", indexesUsed);
  }

  private boolean evaluateWhere(final Document record) {
    ++browsed;
    final Object result = select.rootTreeElement.eval(record);
    if (result instanceof Boolean)
      return (Boolean) result;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }
}
