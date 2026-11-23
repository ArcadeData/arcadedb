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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.LongRangeList;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lines up what a row holds with what the statement deleted, so a node or relationship deleted earlier in the statement and
 * held by the row in another alias, a list, a map or a path is read as {@link DeletedEntityMarker} - and so raises
 * {@code DeletedEntityAccess} - instead of answering with the state it had before the delete (issue #12548). The entity bound
 * to the variable the clause deleted is replaced by the marker itself; this is the rest of the row.
 * <p>
 * A list, a map or a path is not copied and not walked: it is wrapped, once per row, in a live view that asks the
 * {@link Deletions} each time an element is read. So a row costs one wrapper per collection it holds, whatever the size of the
 * collection, where a copy of it would cost its whole length each time a row of a DELETE that removes one element per row
 * shares it ({@code WITH collect(n) AS ns UNWIND ns AS x DELETE x} is a quadratic statement the other way). A view is live: a
 * row buffered before a later row deleted an element it holds reads the deletion too, with nothing to re-mark.
 * <p>
 * A list or a map that holds no record, no path and no collection - numbers, strings, a lazy range - is left as it is, the
 * very object the row came with: it can never hold a deleted entity, and what a later clause does with its type or its
 * identity is not changed by this one. Which lists those are is found once per collection, as the rows that share one share
 * the view of it.
 * <p>
 * Only a read is answered with the marker: {@code size()} of the list, {@code type()} of a relationship in it and the number
 * of elements are unchanged, as they are for the entity the clause deleted.
 * <p>
 * Not covered: a {@link Result} nested in a row value, and a path that reached the row as the {@code {_type: "path"}} map. A
 * value that is neither a record, a list, a map nor a path is left as it is.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class DeletedReferences {
  /** Answers whether the statement deleted a node or a relationship. Asked once per element a row's view hands out. */
  @FunctionalInterface
  public interface Deletions {
    /**
     * @param record a node, a relationship or any other record a row holds
     *
     * @return true when the statement deleted it, or is about to: its DELETE found it connected and left it to the end
     */
    boolean isDeleted(Document record);

    /** @return a check that answers true when this one or {@code other} does; this one itself when {@code other} is null */
    default Deletions or(final Deletions other) {
      if (other == null || other == this) // NOPMD: by identity, the same check twice answers the same
        return this;
      return record -> isDeleted(record) || other.isDeleted(record);
    }
  }

  // The collections of the last rows, so the rows that share one (every row of WITH collect(n) AS ns UNWIND ns AS x) share its
  // view too: whoever keys on a collection by identity, the way HeldCollectionRefresher does, meets one object, not one per row.
  // A few slots, not a map: they are kept until the step closes, and a row of a long statement must not pin its collections
  private static final int MEMO_SIZE = 8;

  private final Deletions deletions;
  private final Object[]  memoSources = new Object[MEMO_SIZE];
  private final Object[]  memoViews   = new Object[MEMO_SIZE];
  private       int       memoNext;

  public DeletedReferences(final Deletions deletions) {
    this.deletions = deletions;
  }

  /**
   * Replaces, in {@code row}, every node and relationship the statement deleted by the marker, and wraps every list, map and
   * path in a view that does the same for what it holds. The row keeps the value itself when it is none of them.
   */
  public void mark(final Result row) {
    if (!(row instanceof ResultInternal internal))
      return;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      final Object marked = value instanceof List || value instanceof Map ? viewOf(value) : resolve(value, deletions);
      if (marked != value) // NOPMD: by identity, as resolve answers an unchanged value
        internal.setProperty(name, marked);
    }
  }

  /** Lets go of the collections the last rows held: called when the step closes. */
  public void clear() {
    for (int i = 0; i < MEMO_SIZE; i++) {
      memoSources[i] = null;
      memoViews[i] = null;
    }
  }

  private Object viewOf(final Object collection) {
    if (collection instanceof LongRangeList)
      return collection; // a lazy range holds numbers only
    for (int i = 0; i < MEMO_SIZE; i++)
      if (memoSources[i] == collection) // NOPMD: by identity
        return memoViews[i];
    // A collection of scalars cannot hold a deleted record: it stays the object it is, so what the rest of the statement does with
    // its type or identity is unchanged, and no row of a long statement pays a wrapper for it
    final Object view = holdsRecordsOrCollections(collection) ? resolve(collection, deletions) : collection;
    memoSources[memoNext] = collection;
    memoViews[memoNext] = view;
    memoNext = (memoNext + 1) % MEMO_SIZE;
    return view;
  }

  /** @return true when a list or a map holds a record, a path or another collection, which can hold one */
  private static boolean holdsRecordsOrCollections(final Object collection) {
    final Iterable<?> elements = collection instanceof Map<?, ?> map ? map.values() : (List<?>) collection;
    for (final Object element : elements)
      if (element instanceof Document || element instanceof TraversalPath || element instanceof List || element instanceof Map)
        return true;
    return false;
  }

  /**
   * @return the marker for a deleted node or relationship, a live view of a list, a map or a path, or {@code value} itself, by
   * identity, when it is none of them
   */
  static Object resolve(final Object value, final Deletions deletions) {
    if (value instanceof Edge edge)
      return deletions.isDeleted(edge) ? new DeletedEntityMarker(edge.getTypeName()) : value;
    if (value instanceof Document document)
      return deletions.isDeleted(document) ? DeletedEntityMarker.INSTANCE : value;
    if (value instanceof List<?> list)
      return list instanceof LongRangeList || list instanceof LiveList live && live.deletions == deletions // NOPMD: by identity
          ? value
          : new LiveList(list, deletions);
    if (value instanceof Map<?, ?> map)
      return map instanceof LiveMap live && live.deletions == deletions ? value : new LiveMap(map, deletions); // NOPMD: by identity
    if (value instanceof TraversalPath path)
      return path.withDeletions(deletions);
    return value;
  }

  /** The elements of a list, each answered as {@link #resolve} does. */
  private static final class LiveList extends AbstractList<Object> {
    private final List<?>   source;
    private final Deletions deletions;

    private LiveList(final List<?> source, final Deletions deletions) {
      this.source = source;
      this.deletions = deletions;
    }

    @Override
    public Object get(final int index) {
      return resolve(source.get(index), deletions);
    }

    @Override
    public int size() {
      return source.size();
    }

    @Override
    public Iterator<Object> iterator() {
      // Not the indexed iteration of AbstractList: a list that is not random access would be walked from its head for each element
      final Iterator<?> iterator = source.iterator();
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public Object next() {
          return resolve(iterator.next(), deletions);
        }
      };
    }
  }

  /** The values of a map, each answered as {@link #resolve} does. */
  private static final class LiveMap extends AbstractMap<Object, Object> {
    private final Map<?, ?> source;
    private final Deletions deletions;

    private LiveMap(final Map<?, ?> source, final Deletions deletions) {
      this.source = source;
      this.deletions = deletions;
    }

    @Override
    public int size() {
      return source.size();
    }

    @Override
    public boolean containsKey(final Object key) {
      return source.containsKey(key);
    }

    @Override
    public Object get(final Object key) {
      return resolve(source.get(key), deletions);
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return new AbstractSet<>() {
        @Override
        public int size() {
          return source.size();
        }

        @Override
        public Iterator<Entry<Object, Object>> iterator() {
          final Iterator<? extends Entry<?, ?>> iterator = source.entrySet().iterator();
          return new Iterator<>() {
            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public Entry<Object, Object> next() {
              final Entry<?, ?> entry = iterator.next();
              return new SimpleImmutableEntry<>(entry.getKey(), resolve(entry.getValue(), deletions));
            }
          };
        }
      };
    }
  }
}
