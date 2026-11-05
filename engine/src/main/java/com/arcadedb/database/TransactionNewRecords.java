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

import java.lang.ref.WeakReference;
import java.util.Arrays;

/**
 * The records a transaction created, in creation order, with the RID each was given (issue #10813). The transaction needs the
 * RIDs for its own bookkeeping, and the records only to reset the identity of one a caller still holds when the transaction, or a
 * statement of it, is rolled back (#4562, #4940).
 * <p>
 * With read-your-writes on, the transaction's record cache holds every new record anyway, so the record is held strongly here too.
 * With it off (the importer, {@code GraphBatch}, any caller that declared it will not read its writes back) nothing else in the
 * transaction does, and holding the record would keep its property map and its serialized buffer alive until the commit: every young
 * collection of a large transaction copied both again. The record is then held weakly: a record the caller dropped is collected,
 * one the caller still holds is still there for the reset, which is the only reader of it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TransactionNewRecords {
  private static final int INITIAL_CAPACITY = 16;

  // A Record, or a WeakReference to one
  private Object[] records = new Object[INITIAL_CAPACITY];
  private RID[]    rids    = new RID[INITIAL_CAPACITY];
  private int      size;

  void add(final Record record, final boolean weak) {
    if (size == records.length) {
      final int capacity = size * 2;
      records = Arrays.copyOf(records, capacity);
      rids = Arrays.copyOf(rids, capacity);
    }
    records[size] = weak ? new WeakReference<>(record) : record;
    rids[size] = record.getIdentity();
    ++size;
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  /** The RID the record at {@code index} was created with. */
  RID getIdentity(final int index) {
    return rids[index];
  }

  /** The record at {@code index}, or null when it was held weakly and nobody holds it any more. */
  Record get(final int index) {
    final Object entry = records[index];
    if (entry instanceof WeakReference<?> reference)
      return (Record) reference.get();
    return (Record) entry;
  }

  /** The position of {@code record} itself (by reference), searched from the end, or -1. */
  int lastIndexOf(final Record record) {
    for (int i = size - 1; i >= 0; i--)
      if (get(i) == record)
        return i;
    return -1;
  }

  void remove(final int index) {
    final int moved = size - index - 1;
    if (moved > 0) {
      System.arraycopy(records, index + 1, records, index, moved);
      System.arraycopy(rids, index + 1, rids, index, moved);
    }
    --size;
    records[size] = null;
    rids[size] = null;
  }

  /**
   * Takes back the records from {@code mark} on, resetting the identity of every one still held to provisional, so the same object
   * can be inserted again.
   */
  void truncateResettingIdentity(final int mark) {
    for (int i = size - 1; i >= mark; i--) {
      resetIdentity(i);
      records[i] = null;
      rids[i] = null;
    }
    size = Math.min(size, mark);
  }

  /** Resets the identity of every record still held to provisional, as a rolled back transaction does. */
  void resetIdentities() {
    for (int i = 0; i < size; i++)
      resetIdentity(i);
  }

  void clear() {
    Arrays.fill(records, 0, size, null);
    Arrays.fill(rids, 0, size, null);
    size = 0;
  }

  private void resetIdentity(final int index) {
    final Record record = get(index);
    if (record != null)
      ((RecordInternal) record).setIdentity(null);
  }
}
