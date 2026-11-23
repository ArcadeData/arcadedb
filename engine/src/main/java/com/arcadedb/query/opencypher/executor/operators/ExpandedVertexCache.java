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

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.RidIntHashMap;

import java.util.Arrays;

/**
 * The vertices one execution of an expansion has already reached, so a vertex reached by many paths is looked up, and its
 * content read, once instead of once per path. A multi-hop MATCH over a dense graph reaches a few thousand vertices through
 * hundreds of thousands of paths: without this every path builds its own instance of the far vertex and reads the record
 * again as soon as a predicate looks at a property of it.
 * <p>
 * It only ever serves a statement that cannot observe the difference, and it gives up as soon as it stops paying:
 * <ul>
 *   <li>the statement is read-only and every read of the thread answers the committed state: no transaction is open, or
 *   the open ones run under READ_COMMITTED and have changed nothing ({@link #readsCommittedStateOnly}). In any other
 *   transaction a lookup answers the transaction's own image of a record (a write, a delete, the snapshot of
 *   REPEATABLE_READ), which an instance kept from an earlier path would hide. Asked again at the start of every batch, and
 *   the cache is dropped for good the first time the answer changes;</li>
 *   <li>the first {@link #WARM_UP_LOOKUPS} lookups are not kept, so a point read or a one-hop expansion allocates
 *   nothing;</li>
 *   <li>it is dropped when fewer than one lookup in four is answered from it over {@link #REVIEW_EVERY} lookups (an
 *   expansion that reaches each vertex once), so such a walk pays a bounded number of misses and nothing after;</li>
 *   <li>it holds at most {@link #MAX_ENTRIES} vertices and {@link #MAX_CONTENT_BYTES} of record content, and is dropped
 *   when the content it pins outgrows that: rows are handed downstream a batch at a time, so without the bound it
 *   would be the only thing keeping large records alive.</li>
 * </ul>
 * Not thread-safe: it belongs to one result set, which one thread pulls.
 */
final class ExpandedVertexCache {
  static final int  WARM_UP_LOOKUPS   = 256;
  static final int  REVIEW_EVERY      = 4_096;
  static final int  MAX_ENTRIES       = 16_384;
  static final long MAX_CONTENT_BYTES = 4L * 1024 * 1024;

  private static final int INITIAL_CAPACITY = 256;

  private RidIntHashMap slots;
  private Vertex[]      vertices;
  // WHETHER THE CONTENT OF THE VERTEX IN THE SLOT IS ALREADY COUNTED IN contentBytes
  private boolean[]     weighed;
  private int           size;
  // ENTRIES BELOW THIS WERE WEIGHED AT THE START OF A BATCH: THE ONES ABOVE WERE ADDED BY THE BATCH THAT IS RUNNING
  private int           sweptUpTo;
  private long          contentBytes;
  private int           warmUpLookups;
  private int           windowLookups;
  private int           windowHits;
  private boolean       armed;
  private boolean       off;

  private ExpandedVertexCache(final boolean eligible) {
    this.off = !eligible;
  }

  /** The cache of an expansion of the statement {@code context} runs: one that is off from the start unless it is read-only. */
  static ExpandedVertexCache of(final CommandContext context) {
    return new ExpandedVertexCache(Boolean.TRUE.equals(context.getVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY)));
  }

  /**
   * Called before each batch of rows is produced. The cache starts, and keeps, answering only while the thread reads the
   * committed state; the records the previous batch added are weighed here, after the rows that carry them went downstream.
   */
  void startBatch(final Database database) {
    if (off || warmUpLookups < WARM_UP_LOOKUPS)
      return;
    if (!readsCommittedStateOnly(database)) {
      drop();
      return;
    }
    armed = true;
    for (int slot = sweptUpTo; slot < size && !off; slot++)
      weigh(slot);
    sweptUpTo = size;
  }

  /**
   * Whether every read this thread makes now answers the committed state, the same on every row: no transaction is open,
   * or the ones that are run under READ_COMMITTED and have changed nothing. A driver's managed read transaction and the
   * auto-commit wrapper of an HTTP command are of that kind. A transaction that wrote anything answers its own image of a
   * record, and REPEATABLE_READ a snapshot, which a value kept from an earlier row would not follow. Every transaction
   * open on the thread is asked: a read nested in a transaction that wrote is not a read of the committed state.
   */
  static boolean readsCommittedStateOnly(final Database database) {
    if (!database.isTransactionActive())
      return true;
    final DatabaseContext.DatabaseContextTL threadContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    if (threadContext == null)
      return false;
    for (final TransactionContext tx : threadContext.transactions)
      if (tx.isActive() && (tx.getIsolationLevel() != Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED || !tx.hasChangedNothing()))
        return false;
    return true;
  }

  /** The vertex kept for {@code rid}, or null when it has to be looked up. */
  Vertex get(final RID rid) {
    if (!armed) {
      if (!off)
        ++warmUpLookups;
      return null;
    }

    ++windowLookups;
    final int slot = slots == null ? -1 : slots.get(rid.getBucketId(), rid.getPosition(), -1);
    if (slot < 0) {
      if (windowLookups >= REVIEW_EVERY) {
        if ((long) windowHits * 4 < windowLookups)
          drop();
        windowLookups = 0;
        windowHits = 0;
      }
      return null;
    }

    ++windowHits;
    final Vertex vertex = vertices[slot];
    weigh(slot);
    return vertex;
  }

  /** Keeps {@code vertex}, just looked up for {@code rid}, when the cache is answering and has room. */
  void put(final RID rid, final Vertex vertex) {
    if (!armed || size >= MAX_ENTRIES)
      return;

    if (slots == null) {
      slots = new RidIntHashMap(INITIAL_CAPACITY);
      vertices = new Vertex[INITIAL_CAPACITY];
      weighed = new boolean[INITIAL_CAPACITY];
    } else if (size == vertices.length) {
      final int grown = Math.min(MAX_ENTRIES, size << 1);
      vertices = Arrays.copyOf(vertices, grown);
      weighed = Arrays.copyOf(weighed, grown);
    }
    slots.put(rid.getBucketId(), rid.getPosition(), size);
    vertices[size++] = vertex;
  }

  /** Counts the content the vertex in {@code slot} holds, once it has any, and drops the cache when it holds too much. */
  private void weigh(final int slot) {
    if (weighed[slot])
      return;
    final int loaded = vertices[slot] instanceof BaseRecord record ? record.getLoadedSize() : 0;
    if (loaded == 0)
      return;
    weighed[slot] = true;
    contentBytes += loaded;
    if (contentBytes > MAX_CONTENT_BYTES)
      drop();
  }

  private void drop() {
    off = true;
    armed = false;
    slots = null;
    vertices = null;
    weighed = null;
    size = 0;
  }

  boolean isAnswering() {
    return armed;
  }

  int size() {
    return size;
  }

  long getContentBytes() {
    return contentBytes;
  }
}
