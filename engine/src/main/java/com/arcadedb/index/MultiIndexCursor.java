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
  private final boolean            ascendingOrder;
  private       int                browsed         = 0;
  private       Object[]           nextKeys;
  private       int                nextCursorIndex = -1;
  private       List<Identifiable> cursorsNextValues;

  public MultiIndexCursor(final List<IndexCursor> cursors, final int limit, final boolean ascendingOrder) {
    this.cursors = cursors;
    this.limit = limit;
    this.ascendingOrder = ascendingOrder;
    this.keyTypes = cursors.get(0).getKeyTypes();
    initCursors();
  }

  public MultiIndexCursor(final List<IndexInternal> indexes, final boolean ascendingOrder, final int limit) {
    this.cursors = new ArrayList<>(indexes.size());
    this.limit = limit;
    for (Index i : indexes) {
      if (!(i instanceof RangeIndex))
        throw new IllegalArgumentException("Cannot iterate an index that does not support ordered iteration");

      this.cursors.add(((RangeIndex) i).iterator(ascendingOrder));
    }
    this.ascendingOrder = ascendingOrder;
    this.keyTypes = indexes.get(0).getBinaryKeyTypes();
    initCursors();
  }

  public MultiIndexCursor(final List<IndexInternal> indexes, final Object[] fromKeys, final boolean ascendingOrder, final boolean includeFrom,
      final int limit) {
    this.cursors = new ArrayList<>(indexes.size());
    this.limit = limit;
    for (Index i : indexes) {
      if (!(i instanceof RangeIndex))
        throw new IllegalArgumentException("Cannot iterate an index that does not support ordered iteration");

      this.cursors.add(((RangeIndex) i).iterator(ascendingOrder, fromKeys, includeFrom));
    }
    this.ascendingOrder = ascendingOrder;
    this.keyTypes = indexes.get(0).getBinaryKeyTypes();
    initCursors();
  }

  @Override
  public Object[] getKeys() {
    return nextKeys;
  }

  @Override
  public Identifiable getRecord() {
    return cursors.get(nextCursorIndex).getRecord();
  }

  @Override
  public boolean hasNext() {
    if (limit > -1 && browsed > limit)
      return false;

    for (int i = 0; i < cursors.size(); ++i) {
      final IndexCursor cursor = cursors.get(i);
      if (cursor != null && (cursorsNextValues.get(i) != null || cursor.hasNext()))
        return true;
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
    if (cursors.get(nextCursorIndex).hasNext())
      cursorsNextValues.set(nextCursorIndex, cursors.get(nextCursorIndex).next());

    return nextValue;
  }

  @Override
  public int getScore() {
    return -1;
  }

  @Override
  public void close() {
    for (IndexCursor cursor : cursors)
      cursor.close();
  }

  @Override
  public String dumpStats() {
    return "no-stats";
  }

  @Override
  public long size() {
    long tot = 0;
    for (IndexCursor cursor : cursors)
      tot += cursor.size();
    return tot;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }

  @Override
  public BinaryComparator getComparator() {
    for (IndexCursor cursor : cursors) {
      if (cursor != null && cursor.hasNext())
        return cursor.getComparator();
    }
    return null;
  }

  @Override
  public byte[] getKeyTypes() {
    for (IndexCursor cursor : cursors) {
      if (cursor != null && cursor.hasNext())
        return cursor.getKeyTypes();
    }
    return null;
  }

  private void initCursors() {
    cursorsNextValues = new ArrayList<>(cursors.size());
    for (int i = 0; i < cursors.size(); ++i) {
      cursorsNextValues.add(null);

      final IndexCursor cursor = cursors.get(i);
      if (cursor == null)
        continue;

      if (!cursor.hasNext()) {
        cursors.set(i, null);
        continue;
      }

      cursorsNextValues.set(i, cursor.next());
    }
  }
}
