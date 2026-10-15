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
package com.arcadedb.server.ha.raft;

import java.util.Arrays;

/**
 * The log indexes of the most recent schema-changing entries a node applied on one database, bounded (issue #10067): a ring of
 * primitives, sorted, with no object per entry. {@link ArcadeStateMachine#lastSchemaChangeIndexOf} says WHEN the schema last changed,
 * which is what the leader's check needs; this says HOW MANY changes landed in a stretch of the log, which is what a replica that
 * asked the leader for one schema change needs to know it is the only one it has to account for.
 * <p>
 * Appended to by the apply thread once per schema entry and read by a client thread once per type a replica asks for, so a monitor is
 * all the synchronization it needs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SchemaChangeLog {
  private final long[] indexes;
  private int          size;

  SchemaChangeLog(final int capacity) {
    this.indexes = new long[capacity];
  }

  /** Records a schema change applied at {@code index}. A change already recorded is not counted twice. */
  synchronized void add(final long index) {
    int position = Arrays.binarySearch(indexes, 0, size, index);
    if (position >= 0)
      return;
    position = -position - 1;

    if (size < indexes.length) {
      System.arraycopy(indexes, position, indexes, position + 1, size - position);
      indexes[position] = index;
      ++size;
    } else if (position > 0) {
      // Full: the oldest is dropped to make room. Everything before the new position moves one place down
      System.arraycopy(indexes, 1, indexes, 0, position - 1);
      indexes[position - 1] = index;
    }
    // else: older than everything it still holds, and the log is full: there is nothing to make room for it with. It is not lost
    // by accident - countBetween() already refuses a stretch that starts before the oldest it holds
  }

  /**
   * How many changes were applied with a log index in {@code (fromExclusive, toInclusive]}, or {@link Integer#MAX_VALUE} when the
   * log is full and its oldest entry is later than {@code fromExclusive}: an older change may have been dropped from the stretch.
   */
  synchronized int countBetween(final long fromExclusive, final long toInclusive) {
    if (size == indexes.length && indexes[0] > fromExclusive)
      return Integer.MAX_VALUE;
    return firstAfter(toInclusive) - firstAfter(fromExclusive);
  }

  /** The position of the first entry greater than {@code value}. */
  private int firstAfter(final long value) {
    final int position = Arrays.binarySearch(indexes, 0, size, value);
    return position >= 0 ? position + 1 : -position - 1;
  }
}
