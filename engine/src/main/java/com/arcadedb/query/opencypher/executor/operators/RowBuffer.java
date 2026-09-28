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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.ImmutableEdge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.query.sql.executor.HeapElementsLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The rows a join operator holds to replay one of its inputs: the right side of a {@link CartesianProduct}, the build
 * side of a {@link ValueHashJoin}.
 * <p>
 * A row as an operator produces it carries every record it binds in full - a vertex with its whole record buffer -
 * while the rows downstream usually read a handful of properties of it. Holding the whole input that way cost about
 * 1.25 KB per vertex, so one product over a large label held gigabytes (issue #8583). The first
 * {@link #DEFAULT_COMPACT_AFTER_ROWS} rows are still held as they came, which is the fastest to replay and costs little;
 * past that the buffer turns compact: every row is held column by column, a record read from storage by its RID alone,
 * in two arrays of primitives, and a value that is not such a record as it is. A compact row is rebuilt when it is
 * read, loading its records again, so a big buffer trades a record load per replayed row for about 12 bytes per record
 * instead of a kilobyte.
 * <p>
 * Only a buffer whose statement does not write is made compact: a write could change what a record reads between the
 * time the row was buffered and the time it is replayed, and the result must not depend on how many rows the buffer
 * happened to hold. A record deleted by another transaction since it was buffered can no longer be loaded, and its
 * row is gone: {@link #get(int)} answers null for it, which is what a nested loop reading the input again would see.
 * <p>
 * Every row counts against {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} (issue
 * #8585), compact or not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RowBuffer {
  /** Past this many rows the buffer holds its rows compact. */
  public static final int DEFAULT_COMPACT_AFTER_ROWS = 10_000;

  private final Database          database;
  private final HeapElementsLimit limit;
  private final int               compactAfterRows;

  private Result[] rows = new Result[16];
  private int      size = 0;

  // Compact form, column by column. A column's arrays are allocated the first time it holds that kind of value.
  private boolean  compact = false;
  private String[] columns;
  private int      capacity;
  /** Per column, the bucket of the record each row binds, or -1 when the row's value is in {@link #objects}. */
  private int[][]  buckets;
  private long[][] positions;
  /** Per column, the values that are not records read from storage; a null entry is a null value. */
  private Object[][]           objects;
  /** Rows whose columns are not the ones of the first row, held as they came. */
  private Map<Integer, Result> irregularRows;

  /**
   * @param database         the database to load the records of a compact row from, or null to never compact
   * @param limit            the element cap of the operation that owns the buffer
   * @param compactAfterRows the rows held as they came before the buffer turns compact; a non-positive value never
   *                         compacts
   */
  public RowBuffer(final Database database, final HeapElementsLimit limit, final int compactAfterRows) {
    this.database = database;
    this.limit = limit;
    this.compactAfterRows = database == null ? 0 : compactAfterRows;
  }

  public int size() {
    return size;
  }

  /** Whether the rows are held compact. */
  public boolean isCompact() {
    return compact;
  }

  public void add(final Result row) {
    limit.check(size + 1L);

    if (!compact) {
      if (compactAfterRows <= 0 || size < compactAfterRows) {
        if (size == rows.length)
          rows = Arrays.copyOf(rows, size + (size >> 1) + 1);
        rows[size++] = row;
        return;
      }
      toCompact();
    }

    ensureCapacity(size + 1);
    store(size, row);
    ++size;
  }

  /**
   * Returns the row at {@code index}, rebuilt with its records loaded again when the buffer is compact, or null when a
   * record the row binds no longer exists.
   */
  public Result get(final int index) {
    if (!compact)
      return rows[index];

    if (irregularRows != null) {
      final Result irregular = irregularRows.get(index);
      if (irregular != null)
        return irregular;
    }

    final ResultInternal row = new ResultInternal();
    for (int column = 0; column < columns.length; column++) {
      final int[] bucketColumn = buckets[column];
      final Object value;
      if (bucketColumn != null && bucketColumn[index] >= 0) {
        try {
          value = database.lookupByRID(new RID(bucketColumn[index], positions[column][index]), true);
        } catch (final RecordNotFoundException e) {
          // Deleted since it was buffered: a nested loop reading the input again would not find it either
          return null;
        }
      } else
        value = objects[column] == null ? null : objects[column][index];
      row.setProperty(columns[column], value);
    }
    return row;
  }

  public void clear() {
    rows = new Result[16];
    size = 0;
    compact = false;
    columns = null;
    buckets = null;
    positions = null;
    objects = null;
    irregularRows = null;
    capacity = 0;
  }

  private void toCompact() {
    final Set<String> names = rows[0].getPropertyNames();
    columns = names.toArray(new String[0]);
    buckets = new int[columns.length][];
    positions = new long[columns.length][];
    objects = new Object[columns.length][];
    capacity = 0;
    ensureCapacity(size + (size >> 1) + 1);

    for (int i = 0; i < size; i++) {
      store(i, rows[i]);
      rows[i] = null;
    }
    rows = null;
    compact = true;
  }

  private void store(final int index, final Result row) {
    final Set<String> names = row.getPropertyNames();
    if (!hasTheColumns(names)) {
      if (irregularRows == null)
        irregularRows = new HashMap<>();
      irregularRows.put(index, row);
      return;
    }

    for (int column = 0; column < columns.length; column++) {
      final Object value = row.getProperty(columns[column]);
      if (isReloadable(value)) {
        final RID rid = ((Identifiable) value).getIdentity();
        if (buckets[column] == null) {
          buckets[column] = new int[capacity];
          Arrays.fill(buckets[column], -1);
          positions[column] = new long[capacity];
        }
        buckets[column][index] = rid.getBucketId();
        positions[column][index] = rid.getPosition();
      } else {
        if (buckets[column] != null)
          buckets[column][index] = -1;
        if (value != null) {
          if (objects[column] == null)
            objects[column] = new Object[capacity];
          objects[column][index] = value;
        }
      }
    }
  }

  private boolean hasTheColumns(final Set<String> names) {
    if (names.size() != columns.length)
      return false;
    final Iterator<String> iterator = names.iterator();
    for (final String column : columns)
      if (!column.equals(iterator.next()))
        return false;
    return true;
  }

  /**
   * A vertex or an edge read from storage, which loading its RID gives back: an embedded document, a lightweight
   * edge or a vertex of a Graph Analytical View has no record to load and is held as it is.
   */
  private static boolean isReloadable(final Object value) {
    if (value == null)
      return false;
    final Class<?> type = value.getClass();
    if (type != ImmutableVertex.class && type != ImmutableEdge.class)
      return false;
    final RID rid = ((Identifiable) value).getIdentity();
    return rid != null && rid.getBucketId() >= 0 && rid.getPosition() >= 0;
  }

  private void ensureCapacity(final int required) {
    if (required <= capacity)
      return;
    final int newCapacity = Math.max(required, capacity + (capacity >> 1) + 16);
    for (int column = 0; column < columns.length; column++) {
      if (buckets[column] != null) {
        final int previous = buckets[column].length;
        buckets[column] = Arrays.copyOf(buckets[column], newCapacity);
        Arrays.fill(buckets[column], previous, newCapacity, -1);
        positions[column] = Arrays.copyOf(positions[column], newCapacity);
      }
      if (objects[column] != null)
        objects[column] = Arrays.copyOf(objects[column], newCapacity);
    }
    capacity = newCapacity;
  }
}
