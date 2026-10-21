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

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The edges of unidirectional types a transaction created or deleted, kept by its {@code TransactionContext} so that a
 * query's {@link IncomingEdgeLookup} can stay on the scan it took and add what its own transaction changed since,
 * rather than scan the type again after every write (issue #8625).
 * <p>
 * Nothing is recorded until a query of the transaction takes a scan ({@link #scanTaken()}): a scan taken later sees
 * the earlier writes by itself, so a transaction that writes such edges and never reads their incoming side - a bulk
 * load - keeps nothing. Every change recorded takes the next value of a sequence, so a scan knows which changes came
 * after it. The changes live until the transaction ends: {@link #transactionEnded()} drops them and moves to the next
 * transaction, and a scan taken in an ended transaction is taken again, since the changes it missed are now committed
 * or rolled back.
 * <p>
 * Changes of other transactions are never seen here: a query keeps reading the scan it took, as it keeps reading the
 * records it already loaded. A nested transaction has a context, and so changes, of its own: a scan taken in the outer
 * transaction does not see the edges an inner one writes.
 * <p>
 * Not thread-safe: a transaction belongs to one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UnidirectionalEdgeChanges {
  /** An edge created in the transaction, the endpoints as the edge list stores them. */
  record Created(long sequence, Edge edge, RID source) {
  }

  // PAST THIS MANY CHANGES THE LOG IS DROPPED AND THE SCANS TAKEN AGAIN: A LARGE WRITE MUST NOT DOUBLE ITS HEAP HERE
  static final int                                MAX_CHANGES = 100_000;

  private long                                    sequence;
  private long                                    overflows;
  private int                                     changes;
  private long                                    transaction;
  private boolean                                 recording;
  // BY TYPE, THEN BY TARGET: A LOOKUP READS THE CHANGES OF ONE TYPE INTO ONE VERTEX
  private Map<String, Map<RID, List<Created>>> created;
  // THE RECORD-BACKED EDGES DELETED: ONE EDGE, ONE ADDRESS, ONE ENTRY
  private Map<RID, Long>                        deleted;
  // THE LIGHTWEIGHT EDGES DELETED, BY TRIPLE (TYPE, OUT, IN): THE TWINS OF A PAIR SHARE ONE IDENTITY, SO A DELETE CANNOT NAME ITS TWIN
  // AND EVERY DELETE OF THE TRIPLE IS KEPT, TO TAKE ONE TWIN OUT OF A LOOKUP EACH (ISSUE #10376)
  private Map<RID, Tombstones>                  deletedTwins;
  // THE SCANS THE DELETES OF THE TRANSACTION SHARE (ISSUE #8676): DROPPED WITH THE CHANGES WHEN THE TRANSACTION ENDS
  private IncomingEdgeLookup                    deleteLookup;
  // THE TYPES WERE TOO LARGE TO INDEX IN HEAP: THE NEXT DELETES OF THE TRANSACTION SCAN FOR THEIR VERTEX AT ONCE
  private boolean                               deleteLookupTooLarge;

  /** The sequence of the last change, in any transaction of the context holding this object. */
  public long getSequence() {
    return sequence;
  }

  /** The number of the current transaction of the context: a scan taken in another one is not covered. */
  public long getTransaction() {
    return transaction;
  }

  /** How many times the log overflowed: a scan taken before the last overflow is no longer covered by it. */
  public long getOverflows() {
    return overflows;
  }

  /** Whether a query of the current transaction took a scan, so its changes have to be recorded. */
  public boolean isRecording() {
    return recording;
  }

  /** A query of the current transaction took a scan: record the changes from now on. */
  public void scanTaken() {
    recording = true;
  }

  public void edgeCreated(final String typeName, final Edge edge, final RID source, final RID target) {
    if (!recording)
      return;
    if (created == null)
      created = new HashMap<>();
    created.computeIfAbsent(typeName, k -> new HashMap<>()).computeIfAbsent(target, k -> new ArrayList<>(2))
        .add(new Created(++sequence, edge, source));
    changeRecorded();
  }

  public void edgeDeleted(final RID edgeIdentity) {
    if (!recording)
      return;
    if (edgeIdentity.getPosition() < 0) {
      // A LIGHTWEIGHT EDGE: KEPT BY ITS TRIPLE ALONE, WITHOUT WHAT THE ITERATION THAT READ IT LEFT ON IT (ITS CENSUS OF THE WHOLE LIST)
      if (deletedTwins == null)
        deletedTwins = new HashMap<>();
      deletedTwins.computeIfAbsent(edgeIdentity instanceof LightEdgeRID light ? light.withoutOrigin() : edgeIdentity,
          k -> new Tombstones()).add(++sequence);
    } else {
      if (deleted == null)
        deleted = new HashMap<>();
      deleted.put(edgeIdentity, ++sequence);
    }
    changeRecorded();
  }

  /**
   * Past {@link #MAX_CHANGES} the log stops: it is dropped and the scans taken so far are taken again at their next use,
   * which sees every change by itself. Recording resumes with the next scan.
   */
  private void changeRecorded() {
    if (++changes > MAX_CHANGES) {
      created = null;
      deleted = null;
      deletedTwins = null;
      changes = 0;
      recording = false;
      ++overflows;
    }
  }

  /** The lookup the vertex deletes of the current transaction share, created on first use (issue #8676). */
  IncomingEdgeLookup getDeleteLookup() {
    if (deleteLookup == null)
      deleteLookup = new IncomingEdgeLookup(true);
    return deleteLookup;
  }

  boolean isDeleteLookupTooLarge() {
    return deleteLookupTooLarge;
  }

  void markDeleteLookupTooLarge() {
    deleteLookupTooLarge = true;
  }

  /** Drops the changes of the transaction that ended: committed or rolled back, they are no longer its own. */
  public void transactionEnded() {
    // NO CONTEXT BUILT IT, SO ITS SNAPSHOTS CHARGED NO QUERY BUDGET THAT HAS TO BE GIVEN BACK: DROPPED AS THEY ARE
    deleteLookup = null;
    deleteLookupTooLarge = false;
    created = null;
    deleted = null;
    deletedTwins = null;
    changes = 0;
    recording = false;
    ++transaction;
  }

  /** The changes kept, for tests. */
  int size() {
    return (created == null ? 0 : created.values().stream().mapToInt(m -> m.values().stream().mapToInt(List::size).sum()).sum())
        + (deleted == null ? 0 : deleted.size()) + (deletedTwins == null ? 0 : deletedTwins.values().stream().mapToInt(t -> t.size).sum());
  }

  /** The edges of {@code typeName} into {@code target} the transaction created, in creation order. */
  List<Created> createdInto(final String typeName, final RID target) {
    if (created == null)
      return Collections.emptyList();
    final Map<RID, List<Created>> byTarget = created.get(typeName);
    if (byTarget == null)
      return Collections.emptyList();
    final List<Created> list = byTarget.get(target);
    return list != null ? list : Collections.emptyList();
  }

  /**
   * Whether the record-backed edge was deleted after {@code afterSequence}. A lightweight edge has no address of its own,
   * so one delete cannot be told from another of its twins: ask {@link #twinsDeletedAfter} for those.
   */
  boolean isDeletedAfter(final RID edgeIdentity, final long afterSequence) {
    if (deleted == null)
      return false;
    final Long at = deleted.get(edgeIdentity);
    return at != null && at > afterSequence;
  }

  /** How many lightweight edges of the triple {@code twin} stands for were deleted after {@code afterSequence}. */
  int twinsDeletedAfter(final RID twin, final long afterSequence) {
    if (deletedTwins == null)
      return 0;
    final Tombstones tombstones = deletedTwins.get(twin);
    return tombstones != null ? tombstones.after(afterSequence) : 0;
  }

  boolean hasDeletions() {
    return deleted != null && !deleted.isEmpty();
  }

  boolean hasTwinDeletions() {
    return deletedTwins != null && !deletedTwins.isEmpty();
  }

  /** The sequences of the deletes of one triple, ascending as they are recorded. */
  private static final class Tombstones {
    private long[] sequences = new long[2];
    private int    size;

    void add(final long sequence) {
      if (size == sequences.length)
        sequences = Arrays.copyOf(sequences, size << 1);
      sequences[size++] = sequence;
    }

    /** How many of the sequences are above {@code afterSequence}: they ascend, so the first one that is is found by halving. */
    int after(final long afterSequence) {
      int low = 0;
      int high = size;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        if (sequences[mid] > afterSequence)
          high = mid;
        else
          low = mid + 1;
      }
      return size - low;
    }
  }
}
