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
package com.arcadedb.index;

import com.arcadedb.database.Identifiable;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.serializer.BinaryComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiIndexCursor implements IndexCursor {
  private final List<IndexCursor>  cursors;
  private final int                limit;
  private final byte[]             keyTypes;
  // #5662: sampled once at construction, like keyTypes. A child's comparator does not depend on how much of it is
  // left, and since #5635 hasNext() prefetches - so probing the children to answer an accessor would run page reads
  // and tombstone skips on a cursor that may only ever be asked for its key types.
  private final BinaryComparator   comparator;
  private final boolean            ascendingOrder;
  private       int                browsed         = 0;
  private       Object[]           nextKeys;
  private       int                nextCursorIndex = -1;
  private       Identifiable       currentRecord;
  private       List<Identifiable> cursorsNextValues;

  public MultiIndexCursor(final List<IndexCursor> cursors, final int limit, final boolean ascendingOrder) {
    this.cursors = cursors;
    this.limit = limit;
    this.ascendingOrder = ascendingOrder;
    try {
      this.keyTypes = cursors.getFirst().getBinaryKeyTypes();
      this.comparator = firstComparator(cursors);
      initCursors();
    } catch (final RuntimeException e) {
      // #5662: the caller handed the children over, so a constructor that does not complete must not leave them open -
      // no one else holds a reference to them
      closeQuietly();
      throw e;
    }
  }

  public MultiIndexCursor(final List<IndexInternal> indexes, final boolean ascendingOrder, final int limit) {
    this.cursors = new ArrayList<>(indexes.size());
    this.limit = limit;
    this.ascendingOrder = ascendingOrder;
    try {
      for (final Index i : indexes) {
        if (!(i instanceof RangeIndex))
          throw new IllegalArgumentException("Cannot iterate an index that does not support ordered iteration");

        this.cursors.add(((RangeIndex) i).iterator(ascendingOrder));
      }
      this.keyTypes = indexes.getFirst().getBinaryKeyTypes();
      this.comparator = firstComparator(this.cursors);
      initCursors();
    } catch (final RuntimeException e) {
      closeQuietly();
      throw e;
    }
  }

  public MultiIndexCursor(final List<IndexInternal> indexes, final Object[] fromKeys, final boolean ascendingOrder,
      final boolean includeFrom, final int limit) {
    this.cursors = new ArrayList<>(indexes.size());
    this.limit = limit;
    this.ascendingOrder = ascendingOrder;
    try {
      for (final Index i : indexes) {
        if (!(i instanceof RangeIndex))
          throw new IllegalArgumentException("Cannot iterate an index that does not support ordered iteration");

        this.cursors.add(((RangeIndex) i).iterator(ascendingOrder, fromKeys, includeFrom));
      }
      this.keyTypes = indexes.getFirst().getBinaryKeyTypes();
      this.comparator = firstComparator(this.cursors);
      initCursors();
    } catch (final RuntimeException e) {
      closeQuietly();
      throw e;
    }
  }

  @Override
  public Object[] getKeys() {
    return nextKeys;
  }

  @Override
  public Identifiable getRecord() {
    return currentRecord;
  }

  @Override
  public boolean hasNext() {
    if (limit > -1 && browsed > limit)
      return false;

    for (int i = 0; i < cursors.size(); ++i) {
      final IndexCursor cursor = cursors.get(i);
      if (cursor == null)
        continue;
      if (cursorsNextValues.get(i) != null || cursor.hasNext())
        return true;
      // EXHAUSTED CHILD: CLOSE IT EAGERLY TO RELEASE ITS PAGE CURSORS AND CLEAR THE SLOT
      cursor.close();
      cursors.set(i, null);
    }

    return false;
  }

  @Override
  public Identifiable next() {
    nextCursorIndex = -1;
    nextKeys = null;

    for (int i = 0; i < cursors.size(); ++i) {

      final IndexCursor cursor = cursors.get(i);

      if (cursor == null)
        continue;

      final Identifiable cursorsNextValue = cursorsNextValues.get(i);
      if (cursorsNextValue == null && !cursor.hasNext()) {
        cursor.close();
        cursors.set(i, null);
        continue;
      }

      if (nextCursorIndex == -1) {
        nextCursorIndex = i;
        nextKeys = cursor.getKeys();
        continue;
      }

      final int cmp = LSMTreeIndexMutable.compareKeys(cursor.getComparator(), keyTypes, cursor.getKeys(), nextKeys);
      if (ascendingOrder) {
        if (cmp < 0) {
          nextCursorIndex = i;
          nextKeys = cursor.getKeys();
        }
      } else {
        if (cmp > 0) {
          nextCursorIndex = i;
          nextKeys = cursor.getKeys();
        }
      }
    }

    if (nextCursorIndex < 0)
      throw new NoSuchElementException();

    ++browsed;

    final Identifiable nextValue = cursorsNextValues.set(nextCursorIndex, null);
    currentRecord = nextValue;

    // #5635: refill this cursor's lookahead slot. An IndexCursor whose hasNext() answered true always yields a
    // non-null element, so a live cursor's slot is never left empty - which used to happen with the optimistic
    // hasNext() of LSMTreeIndexCursor and propagated the null straight out of this next().
    final IndexCursor consumed = cursors.get(nextCursorIndex);
    if (consumed.hasNext())
      cursorsNextValues.set(nextCursorIndex, consumed.next());

    return nextValue;
  }

  @Override
  public void close() {
    for (int i = 0; i < cursors.size(); ++i) {
      final IndexCursor cursor = cursors.get(i);
      if (cursor != null) {
        cursor.close();
        cursors.set(i, null);
      }
    }
  }

  /**
   * {@link #close()} for the failed-construction path: it must not mask the exception that is being propagated, and it
   * cannot assume the fields it reads were assigned.
   */
  private void closeQuietly() {
    for (int i = 0; i < cursors.size(); ++i) {
      final IndexCursor cursor = cursors.get(i);
      if (cursor != null) {
        try {
          cursor.close();
        } catch (final RuntimeException ignore) {
          // KEEP CLOSING THE OTHERS
        }
        cursors.set(i, null);
      }
    }
  }

  @Override
  public long estimateSize() {
    long tot = 0L;
    for (final IndexCursor cursor : cursors) {
      if (cursor == null)
        continue;
      if (cursor.estimateSize() == -1)
        return -1;
      tot += cursor.estimateSize();
    }
    return tot;
  }

  public int getCursors() {
    return cursors.size();
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }

  /**
   * The comparator sampled at construction. Reading it does not touch the children: since #5635
   * {@code LSMTreeIndexCursor.hasNext()} prefetches, so the previous "first child that still has something"
   * formulation turned a plain accessor into page reads and tombstone-skip work (#5662).
   */
  @Override
  public BinaryComparator getComparator() {
    return comparator;
  }

  /**
   * The key types sampled at construction, for the same reason as {@link #getComparator()}. Every child scans the
   * same logical index over a different bucket, so they all agree on the key types, exhausted or not.
   */
  @Override
  public byte[] getBinaryKeyTypes() {
    return keyTypes;
  }

  private static BinaryComparator firstComparator(final List<IndexCursor> cursors) {
    for (final IndexCursor cursor : cursors)
      if (cursor != null)
        return cursor.getComparator();
    return null;
  }

  private void initCursors() {
    cursorsNextValues = new ArrayList<>(cursors.size());
    for (Iterator<IndexCursor> c = cursors.iterator(); c.hasNext(); ) {
      final IndexCursor cursor = c.next();
      if (cursor == null)
        c.remove();
      else if (!cursor.hasNext()) {
        cursor.close();
        c.remove();
      } else
        cursorsNextValues.add(cursor.next());
    }
  }
}
