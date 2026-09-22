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
package com.arcadedb.database;

import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;

import java.util.Collection;
import java.util.Iterator;

public class IndexCursorCollection implements IndexCursor {
  private static final Object[] NO_KEYS      = new Object[0];
  private static final byte[]   NO_KEY_TYPES = new byte[0];

  private final Collection<Identifiable> collection;
  private final Iterator<Identifiable>   iterator;
  // #8153: THE KEY EVERY RECORD OF AN EQUALITY LOOKUP SHARES, WITH THE KEY TYPES AND THE COMPARATOR OF THE INDEX IT CAME
  // FROM, SO A MultiIndexCursor MERGING THIS CURSOR WITH RANGE CURSORS OVER THE SAME INDEX CAN ORDER IT AMONG THEM
  private final Object[]                 keys;
  private final byte[]                   binaryKeyTypes;
  private final BinaryComparator         comparator;
  private       Identifiable             last = null;

  public IndexCursorCollection(final Collection<Identifiable> collection) {
    this(collection, NO_KEYS, NO_KEY_TYPES, null);
  }

  public IndexCursorCollection(final Collection<Identifiable> collection, final Object[] keys, final byte[] binaryKeyTypes,
      final BinaryComparator comparator) {
    this.collection = collection;
    this.iterator = collection.iterator();
    this.keys = keys;
    this.binaryKeyTypes = binaryKeyTypes;
    this.comparator = comparator;
  }

  @Override
  public Object[] getKeys() {
    return keys;
  }

  @Override
  public Identifiable getRecord() {
    return last;
  }

  @Override
  public BinaryComparator getComparator() {
    return comparator;
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return binaryKeyTypes;
  }

  @Override
  public long estimateSize() {
    return collection.size();
  }

  /**
   * #5662: a cursor iterates ITSELF, like every other {@link IndexCursor}. Handing back the backing iterator shared
   * the position with {@link #next()} but bypassed it, so {@link #getRecord()} stayed stale for the whole for-each.
   * <p>
   * A cursor is single-pass either way: the backing iterator was created once in the constructor, so the old
   * implementation handed back the SAME exhausted iterator on a second call rather than a fresh traversal.
   */
  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Identifiable next() {
    last = iterator.next();
    return last;
  }
}
