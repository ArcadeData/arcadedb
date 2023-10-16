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
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Query iterator returned from queries. Extends the base Java iterator with convenient methods.
 *
 * <h4>Implementation details</h4>
 * <p>
 * The iterator keeps track for the returned records in case multiple indexes have being used. In fact, in case multiple
 * indexes are used, it's much simpler to just return index cursor tha could overlap. In this case, the property
 * `filterOutRecords` keeps track of the returning RIDs to avoid returning duplicates.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectIterator<T extends Identifiable> implements Iterator<T> {
  private final SelectExecutor         executor;
  private final Iterator<Identifiable> iterator;
  private       HashSet<RID>           filterOutRecords;
  private       T                      next       = null;
  private       long                   returned   = 0;
  private       List<Document>         sortedResultSet;
  private       int                    orderIndex = 0;

  protected SelectIterator(final SelectExecutor executor, final Iterator<Identifiable> iterator,
      final boolean enforceUniqueReturn) {
    this.executor = executor;
    this.iterator = iterator;
    if (enforceUniqueReturn)
      this.filterOutRecords = new HashSet<>();

    fetchResultInCaseOfOrderBy();

    for (int i = 0; i < executor.select.skip; i++) {
      // CONSUME UNTIL THE SKIP THRESHOLD IS LIMIT
      if (hasNext())
        next();
    }
  }

  @Override
  public boolean hasNext() {
    if (sortedResultSet != null) {
      // RETURN FROM THE ORDERED RESULT SET
      final boolean more = orderIndex < sortedResultSet.size();
      if (!more)
        // EARLY DISPOSAL OF SORTED RESULT SET
        sortedResultSet = null;
      return more;
    }

    if (executor.select.limit > -1 && returned >= executor.select.limit)
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
    if (sortedResultSet != null)
      // RETURN FROM THE ORDERED RESULT SET
      return (T) sortedResultSet.get(orderIndex++);

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

      if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
        // ALREADY RETURNED, AVOID DUPLICATES IN THE RESULTSET
        continue;

      if (executor.select.rootTreeElement == null || executor.evaluateWhere(record)) {
        ++returned;

        if (filterOutRecords != null)
          filterOutRecords.add(record.getIdentity());

        return (T) record;
      }

    } while (iterator.hasNext());

    // NOT FOUND
    return null;
  }

  public T nextOrNull() {
    return hasNext() ? next() : null;
  }

  public List<T> toList() {
    final List<T> result = new ArrayList<>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public Map<String, Object> getMetrics() {
    return executor.metrics();
  }

  private void fetchResultInCaseOfOrderBy() {
    if (executor.select.orderBy == null)
      return;

    // CHECK ONLY THE CASE WITH ONE INDEX USED AND ONE ORDER BY
    if (executor.select.orderBy.size() == 1 && executor.usedIndexes != null && executor.usedIndexes.size() == 1) {
      final Pair<String, Boolean> orderBy = executor.select.orderBy.get(0);
      final SelectExecutor.IndexInfo usedIndex = executor.usedIndexes.get(0);

      if (orderBy.getFirst().equals(usedIndex.property) &&//
          orderBy.getSecond() == usedIndex.order) {
        // ORDER BY THE INDEX USED, RESULTSET IS ALREADY ORDERED
        return;
      }
    }

    final List<Document> resultSet = new ArrayList<>();
    while (hasNext())
      resultSet.add(next().asDocument(true));
    sortedResultSet = resultSet;

    Collections.sort(sortedResultSet, (a, b) -> {
      for (Pair<String, Boolean> orderBy : executor.select.orderBy) {
        final Object aVal = a.get(orderBy.getFirst());
        final Object bVal = b.get(orderBy.getFirst());
        int comp = BinaryComparator.compareTo(aVal, bVal);
        if (comp != 0) {
          if (!orderBy.getSecond())
            comp *= -1;
          return comp;
        }
      }
      return 0;
    });
  }
}
