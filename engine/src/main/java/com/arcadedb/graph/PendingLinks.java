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
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

import java.util.Arrays;

/**
 * An append-only list of adjacency entries to (re)create, each the triple (vertex whose list gains the entry,
 * edge, the other endpoint the entry records). Shared by the repairs that plan entries before writing any: the
 * {@code CHECK DATABASE FIX} rebuild of {@link GraphDatabaseChecker} and {@link GraphEngine#reconnectEdgesFromSurvivors}
 * (#10407), which moved it out of the checker.
 * <p>
 * PRIMITIVE ARRAYS, not a {@code List} of anything. This is the one structure #6136 adds that grows with the
 * amount of damage, and the accumulation it replaces is the warning: {@code reconnectEdges} held an
 * {@code ArrayList<Edge>} of every edge to re-link, i.e. a fully materialised record - properties and all - per
 * entry, which is unbounded heap in exactly the way the WAL growth this change fixes was unbounded. Three RIDs
 * flattened into an {@code int[]} plus a {@code long[]} is 36 bytes per entry with no per-object header and no
 * card-marking, against a couple of hundred for the record it used to keep alive, and the apply loop rebuilds
 * the RID objects one entry at a time.
 * <p>
 * Still unbounded in the number of ENTRIES, and deliberately so: an entry is dropped only once its repair has
 * been applied, and a cap would mean silently declining to repair part of the damage. The bound that matters -
 * and the one #6128/#6136 are about - is on the transaction, not on the plan.
 * <p>
 * The RIDs it hands back are BOUND TO THE DATABASE ({@code database.newRID}), which is not cosmetic. Flattening
 * to primitives loses the owning database, and a bare {@code RID} resolves one through
 * {@code RID.resolveActiveDatabase()} - a thread-local lookup that THROWS when more than one database is in
 * scope on the thread. That exception would be caught by the apply loop's generic handler and turned into a
 * per-entry warning, so on a multi-database server the repair would silently decline to apply while the run
 * still reported success. Rebuilding them here rather than at each call site means no future caller can
 * reintroduce that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PendingLinks {
  private static final int TRIPLE = 3;

  private final DatabaseInternal database;

  private int[]  buckets   = new int[TRIPLE * 16];
  private long[] positions = new long[TRIPLE * 16];
  private int    size;

  PendingLinks(final DatabaseInternal database) {
    this.database = database;
  }

  void add(final RID vertex, final RID edge, final RID other) {
    final int base = size * TRIPLE;
    if (base + TRIPLE > buckets.length) {
      final int newLength = buckets.length + (buckets.length >> 1) + TRIPLE;
      buckets = Arrays.copyOf(buckets, newLength);
      positions = Arrays.copyOf(positions, newLength);
    }
    set(base, vertex);
    set(base + 1, edge);
    set(base + 2, other);
    ++size;
  }

  private void set(final int slot, final RID rid) {
    buckets[slot] = rid.getBucketId();
    positions[slot] = rid.getPosition();
  }

  private RID get(final int slot) {
    return database.newRID(buckets[slot], positions[slot]);
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  RID vertex(final int i) {
    return get(i * TRIPLE);
  }

  RID edge(final int i) {
    return get(i * TRIPLE + 1);
  }

  RID other(final int i) {
    return get(i * TRIPLE + 2);
  }
}
