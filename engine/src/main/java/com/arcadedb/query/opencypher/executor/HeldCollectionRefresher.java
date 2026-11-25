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
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lines up the records a row's lists and maps hold with the writes the statement's SET and REMOVE clauses made, so a node read
 * back through {@code collect()}, a list literal or a map in the same statement answers with the written state (issue
 * #11754): {@code WITH collect(n) AS ns SET (head(ns)).v = 10 RETURN head(ns).v} read 1. {@link RowAliases#propagateUpdate}
 * points the row's own aliases and paths at the written record and deliberately stops there; this is the descent it leaves
 * out. A clause asks it twice per row: before it evaluates anything, so a list shared with an earlier row reads what that row
 * wrote ({@code WITH [n] AS ns UNWIND [1, 2] AS i SET (head(ns)).v = head(ns).v + 1} must add 2), and after its writes.
 * <p>
 * The descent is not charged per write the way an alias redirect is. The common shape is one list shared by every row
 * - {@code WITH collect(n) AS ns UNWIND ns AS m SET m.v = 1} - and walking it on every row would make the clause quadratic
 * in the size of the list. So a list or a map is walked once, the first time a row brings it: the walk builds a copy with
 * every record the transaction wrote already replaced, and the positions each record holds in it (a map from record to
 * position for one of at least {@link #MIN_INDEXED_SIZE} elements, a scan for a smaller one). Every later row that brings the
 * same collection, or the copy itself, by identity, gets that same copy, updated in place at the positions of the records
 * written since, so a row costs what was written, not what it holds. The original collection is never written to.
 * <p>
 * One instance serves the whole statement ({@link #of}), every SET and REMOVE clause of it, those of a CALL body or of a
 * FOREACH body included: a write logged by any of them reaches every indexed copy, so a copy a later clause passes on never
 * keeps a record an earlier clause wrote after the copy was built.
 * <p>
 * The copy being shared is what keeps that cost flat, and it is also the semantics: the collection is one value of the
 * rows that carry it, so a write through any of them shows through all of them, exactly as a node bound by several rows
 * is the one record the transaction holds, whatever the size of the collection: an earlier row's copy of a two-element list
 * reads what a later row wrote, as a 2000-element one does. A collection that nests a list, a map or a path, which the
 * positions do not follow, is walked on each row that holds it instead, and copied only when something in it changed. A
 * lazy {@code range()} holds numbers only and is never walked.
 * <p>
 * Only what the transaction wrote replaces a record ({@link WrittenCopy}), never a read-only cached copy: issue #6362 is
 * why. At most {@link #MAX_INDEXED} large and {@link #MAX_SMALL_INDEXED} small collections are indexed at a time, and at
 * most {@link #MAX_LOG} writes wait to be applied: past either of the first two that size class starts over, which costs a
 * collection that comes back one more walk and never a stale record, past the third every copy is brought up to date at
 * once. Not thread-safe: a statement applies its rows one at a time.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public final class HeldCollectionRefresher {
  public static final  int    MIN_INDEXED_SIZE  = 16;
  private static final int    MAX_INDEXED       = 32;
  private static final int    MAX_SMALL_INDEXED = 64;
  private static final int    MAX_LOG           = 1024;
  private static final String CONTEXT_KEY       = "$cypher.heldCollections";

  // By the identity of the collection a row brought, and of the copy made from it
  private final IdentityHashMap<Object, Indexed> indexed = new IdentityHashMap<>();
  // Each indexed copy once, and how many of them are small
  private final List<Indexed>                    entries = new ArrayList<>();
  private       int                              smallEntries;
  // The records written since the oldest indexed copy was last brought up to date; kept only while something is indexed
  private final List<Document>                   log     = new ArrayList<>();

  /** The statement's instance, shared by every SET and REMOVE clause it runs, the bodies of its subqueries included. */
  public static HeldCollectionRefresher of(final CommandContext context) {
    if (context == null)
      return new HeldCollectionRefresher();
    if (context.getCachedValue(CONTEXT_KEY) instanceof HeldCollectionRefresher existing)
      return existing;
    final HeldCollectionRefresher created = new HeldCollectionRefresher();
    context.setCachedValue(CONTEXT_KEY, created);
    return created;
  }

  /**
   * Makes a nested plan's context answer {@link #of} with the enclosing statement's instance, so a write inside a
   * {@code CALL { }} body reaches the copies the outer clauses indexed, as {@link LabelReplacements#inherit} does for a
   * label write.
   */
  public static void inherit(final CommandContext inner, final CommandContext outer) {
    if (inner == null || outer == null)
      return;
    inner.setCachedValue(CONTEXT_KEY, of(outer));
  }

  /** Records that a clause wrote {@code record}: the version every collection holding it must answer with. */
  public void written(final Document record) {
    // A copy is built from every write the transaction made so far, so nothing needs logging while nothing is indexed. A
    // record with no identity cannot be in any index: positions are keyed by RID
    if (entries.isEmpty() || record.getIdentity() == null)
      return;
    log.add(record);
    if (log.size() >= MAX_LOG) {
      // A copy no row has brought for a while holds the log back: bring every copy up to date and start the log over
      for (final Indexed entry : entries)
        sync(entry);
      log.clear();
      for (final Indexed entry : entries)
        entry.synced = 0;
    }
  }

  /**
   * Replaces, in every list and map of {@code row}, the records the transaction wrote with their written version. A row with
   * no list and no map costs one walk of its property names.
   */
  public void refresh(final Result row) {
    if (!(row instanceof ResultInternal internal))
      return;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      if ((value instanceof List && !(value instanceof LongRangeList)) || value instanceof Map) {
        final Object live = liveCollection(value);
        if (live != value) // NOPMD: by identity, the collection itself is answered when nothing in it changed
          internal.setProperty(name, live);
      }
    }
    compactLog();
  }

  private Object liveCollection(final Object collection) {
    Indexed entry = indexed.get(collection);
    if (entry != null) {
      sync(entry);
      return entry.copy;
    }

    entry = Indexed.build(collection);
    if (entry == null) // it nests a collection or a path: walked on the row that holds it
      return RowAliases.liveValue(collection, WrittenCopy::of);
    // A collection that holds no record is its own copy, and nothing in it can ever be written. A large one is still
    // indexed, inert, so the rows that share it do not scan it again; a small one costs less to scan than to remember
    if (entry.copy == collection && entry.small) // NOPMD: by identity
      return collection;

    if (entry.small ? smallEntries >= MAX_SMALL_INDEXED : entries.size() - smallEntries >= MAX_INDEXED)
      evict(entry.small);
    entry.synced = log.size();
    entries.add(entry);
    if (entry.small)
      smallEntries++;
    indexed.put(collection, entry);
    // A clause after this one meets the copy, not the collection it was made from: it is the same value
    indexed.put(entry.copy, entry);
    return entry.copy;
  }

  /**
   * Forgets every indexed copy of one size class, so a row that builds a small list of its own ({@code WITH n, [n] AS ns})
   * never pushes out the index of a large list every row shares. A collection that comes back is indexed again, from what
   * the transaction wrote, which costs one more walk and never a stale record.
   */
  private void evict(final boolean small) {
    for (int i = entries.size() - 1; i >= 0; i--) {
      final Indexed entry = entries.get(i);
      if (entry.small != small)
        continue;
      entries.remove(i);
      indexed.remove(entry.source);
      indexed.remove(entry.copy);
    }
    if (small)
      smallEntries = 0;
    if (entries.isEmpty())
      log.clear();
  }

  private void sync(final Indexed entry) {
    for (int i = entry.synced; i < log.size(); i++)
      entry.update(log.get(i));
    entry.synced = log.size();
  }

  /** Drops the part of the log every indexed copy has already applied. */
  private void compactLog() {
    if (log.isEmpty())
      return;
    int oldest = log.size();
    for (final Indexed entry : entries)
      oldest = Math.min(oldest, entry.synced);
    if (oldest == 0)
      return;
    log.subList(0, oldest).clear();
    for (final Indexed entry : entries)
      entry.synced -= oldest;
  }

  /** A copy of an indexed collection, with the positions each record holds in it. */
  private static final class Indexed {
    private final Object            source;
    private final Object            copy;
    private final boolean           small;
    // For a map, the key of each position; null for a list
    private final Object[]          keys;
    // The record of each position, null where the element is not one
    private final RID[]             rids;
    // A large collection only: the last position of each record, and for each position the previous one of the same
    // record, or -1. A small one is scanned instead, which costs less than building the map
    private final Map<RID, Integer> lastPosition;
    private final int[]             previous;
    private       int               synced;

    private Indexed(final Object source, final Object copy, final Object[] keys, final RID[] rids,
        final Map<RID, Integer> lastPosition, final int[] previous) {
      this.source = source;
      this.copy = copy;
      this.small = lastPosition == null;
      this.keys = keys;
      this.rids = rids;
      this.lastPosition = lastPosition;
      this.previous = previous;
    }

    /**
     * @return the index of {@code collection}, or null when it nests a list, a map or a path. A collection that holds no
     * record is answered as its own copy: nothing in it can ever be written
     */
    static Indexed build(final Object collection) {
      final Object[] keys;
      final Object[] values;
      if (collection instanceof List<?> list) {
        if (!holdsOnlyRecordsAndScalars(list))
          return null;
        keys = null;
        values = list.toArray();
      } else {
        final Map<?, ?> map = (Map<?, ?>) collection;
        if (!holdsOnlyRecordsAndScalars(map.values()))
          return null;
        keys = new Object[map.size()];
        values = new Object[map.size()];
        int i = 0;
        for (final Map.Entry<?, ?> e : map.entrySet()) {
          keys[i] = e.getKey();
          values[i++] = e.getValue();
        }
      }

      final boolean large = values.length >= MIN_INDEXED_SIZE;
      final RID[] rids = new RID[values.length];
      final Map<RID, Integer> lastPosition = large ? new HashMap<>() : null;
      final int[] previous = large ? new int[values.length] : null;
      boolean holdsRecords = false;
      for (int i = 0; i < values.length; i++) {
        if (values[i] instanceof Document doc && doc.getIdentity() != null) {
          holdsRecords = true;
          rids[i] = doc.getIdentity();
          values[i] = WrittenCopy.of(doc);
          if (large) {
            final Integer last = lastPosition.put(rids[i], i);
            previous[i] = last != null ? last : -1;
          }
        }
      }
      if (!holdsRecords)
        return new Indexed(collection, collection, null, null, large ? Map.of() : null, null);

      final Object copy;
      if (keys == null)
        copy = new ArrayList<>(Arrays.asList(values));
      else {
        final Map<Object, Object> map = new LinkedHashMap<>(keys.length * 4 / 3 + 1);
        for (int i = 0; i < keys.length; i++)
          map.put(keys[i], values[i]);
        copy = map;
      }
      return new Indexed(collection, copy, keys, rids, lastPosition, previous);
    }

    private static boolean holdsOnlyRecordsAndScalars(final Iterable<?> elements) {
      for (final Object element : elements)
        if (element instanceof List || element instanceof Map || element instanceof TraversalPath)
          return false;
      return true;
    }

    void update(final Document written) {
      if (rids == null)
        return; // it holds no record
      final RID rid = written.getIdentity();
      if (lastPosition != null) {
        final Integer last = lastPosition.get(rid);
        if (last != null)
          for (int i = last; i >= 0; i = previous[i])
            set(i, written);
        return;
      }
      for (int i = 0; i < rids.length; i++)
        if (rid.equals(rids[i]))
          set(i, written);
    }

    @SuppressWarnings("unchecked")
    private void set(final int position, final Document written) {
      if (keys == null)
        ((List<Object>) copy).set(position, written);
      else
        ((Map<Object, Object>) copy).put(keys[position], written);
    }
  }
}
