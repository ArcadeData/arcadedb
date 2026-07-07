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
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
public class SelectIterator<T extends Document> implements Iterator<T>, AutoCloseable {
  protected final SelectExecutor                   executor;
  protected final Iterator<? extends Identifiable> iterator;
  protected final Set<RID>                         filterOutRecords;
  private         T                                next       = null;
  protected       long                             returned   = 0;
  private         List<Document>                   sortedResultSet;
  private         int                              orderIndex = 0;
  private         boolean                          orderByMaterialized;

  protected SelectIterator(final SelectExecutor executor, final Iterator<? extends Identifiable> iterator,
      final boolean enforceUniqueReturn) {
    this.executor = executor;
    this.iterator = iterator;
    if (enforceUniqueReturn)
      this.filterOutRecords = ConcurrentHashMap.newKeySet();
    else
      this.filterOutRecords = null;

    // NOTE: THE ORDER BY MATERIALIZATION IS TRIGGERED LAZILY FROM hasNext()/next() (SEE materializeOrderBy()) AND NOT HERE,
    // BECAUSE SUBCLASSES (e.g. SelectParallelIterator) FINISH INITIALIZING THEIR FETCH MACHINERY ONLY AFTER super() RETURNS.
    for (int i = 0; i < executor.select.skip; i++) {
      // CONSUME UNTIL THE SKIP THRESHOLD HITS
      if (hasNext())
        next();
      else
        break;
    }
  }

  @Override
  public boolean hasNext() {
    materializeOrderBy();
    if (sortedResultSet != null) {
      // RETURN FROM THE ORDERED RESULT SET
      final boolean more = orderIndex < sortedResultSet.size();
      if (!more)
        // EARLY DISPOSAL OF SORTED RESULT SET
        sortedResultSet = null;
      return more;
    }

    // THE LIMIT COUNTS THE RECORDS RETURNED AFTER THE SKIPPED ONES (STANDARD SKIP/LIMIT SEMANTICS). `returned` IS
    // ALREADY AT `skip` WHEN THE STREAMING STARTS BECAUSE THE CONSTRUCTOR CONSUMES THE SKIPPED RECORDS THROUGH
    // hasNext()/next(), SO THE CAP IS skip + limit (THE ORDER BY PATH APPLIES THE SAME CAP ON THE SORTED RESULT SET)
    if (executor.select.limit > -1 && returned >= (long) executor.select.limit + executor.select.skip)
      return false;
    if (next != null)
      return true;

    next = fetchNext();
    return next != null;
  }

  @Override
  public T next() {
    materializeOrderBy();
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

  protected T fetchNext() {
    if (!iterator.hasNext())
      return null;

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

  /**
   * Releases the resources associated to the iterator. The serial implementation has nothing to release; the parallel
   * implementation ({@link SelectParallelIterator}) overrides this method to stop the background producers, so an early
   * close does not leave async workers running (#5065). Closing an iterator is optional when it is fully consumed.
   */
  @Override
  public void close() {
    // NOTHING TO RELEASE IN THE SERIAL IMPLEMENTATION
  }

  public List<T> toList() {
    final List<T> result = new ArrayList<>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL), false);
  }

  @SuppressWarnings("unchecked")
  public SelectVertexTraversal outVertices(final String... edgeTypes) {
    return new SelectVertexTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.OUT, edgeTypes);
  }

  @SuppressWarnings("unchecked")
  public SelectVertexTraversal inVertices(final String... edgeTypes) {
    return new SelectVertexTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.IN, edgeTypes);
  }

  @SuppressWarnings("unchecked")
  public SelectVertexTraversal bothVertices(final String... edgeTypes) {
    return new SelectVertexTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.BOTH, edgeTypes);
  }

  @SuppressWarnings("unchecked")
  public SelectEdgeTraversal outEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.OUT, edgeTypes);
  }

  @SuppressWarnings("unchecked")
  public SelectEdgeTraversal inEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.IN, edgeTypes);
  }

  @SuppressWarnings("unchecked")
  public SelectEdgeTraversal bothEdges(final String... edgeTypes) {
    return new SelectEdgeTraversal((Iterator<Vertex>) this, Vertex.DIRECTION.BOTH, edgeTypes);
  }

  public Map<String, Object> getMetrics() {
    return executor.metrics();
  }

  private void materializeOrderBy() {
    if (orderByMaterialized)
      return;
    orderByMaterialized = true;
    fetchResultInCaseOfOrderBy();
  }

  private void fetchResultInCaseOfOrderBy() {
    if (executor.select.orderBy == null)
      return;

    // CHECK ONLY THE CASE WITH ONE INDEX USED AND ONE ORDER BY
    if (executor.select.orderBy.size() == 1 && executor.usedIndexes != null && executor.usedIndexes.size() == 1) {
      final Pair<String, Boolean> orderBy = executor.select.orderBy.getFirst();
      final SelectExecutor.IndexInfo usedIndex = executor.usedIndexes.getFirst();

      if (orderBy.getFirst().equals(usedIndex.property) &&//
          orderBy.getSecond() == usedIndex.order)
        // ORDER BY THE INDEX USED, RESULTSET IS ALREADY ORDERED
        return;
    }

    // MATERIALIZE THE FULL RESULT SET DRAINING THE UNDERLYING ITERATOR VIA fetchNext() (NOT next(), WHICH WOULD READ BACK
    // FROM sortedResultSet), THEN SORT IT IN MEMORY.
    final List<Document> materialized = new ArrayList<>();
    for (T record = fetchNext(); record != null; record = fetchNext())
      materialized.add(record.asDocument(true));

    materialized.sort((a, b) -> {
      for (final Pair<String, Boolean> orderBy : executor.select.orderBy) {
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

    // APPLY THE LIMIT HERE (SKIP IS APPLIED BY THE CONSTRUCTOR CONSUMING THE FIRST `skip` ELEMENTS OF sortedResultSet).
    if (executor.select.limit > -1) {
      final int end = (int) Math.min((long) materialized.size(), (long) executor.select.skip + executor.select.limit);
      sortedResultSet = new ArrayList<>(materialized.subList(0, end));
    } else
      sortedResultSet = materialized;
  }
}
