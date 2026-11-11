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
package com.arcadedb.engine;

import com.arcadedb.database.RID;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The relationships committed by a match-or-create (a Cypher {@code MERGE}, {@code merge.relationship}, SQL
 * {@code CREATE EDGE ... IF NOT EXISTS}) since a given moment, so a transaction that created one of them on the strength of not
 * finding it can learn at its commit that another transaction committed the same one meanwhile (issue #11482).
 * <p>
 * The existence check of such a statement reads an edge list, and nothing a page version can see protects that read: the edge it
 * would have found is APPENDED to a list the statement then appends to as well, and two appends to one list commute at commit
 * (the edge-append and slot merges replay one over the other), so both transactions committed the same relationship and neither
 * learned of the other. The page-level remedy, refusing to merge such pages, would refuse every unrelated append that shares the
 * page, the very retry storm those merges exist to remove.
 * <p>
 * The answer is a predicate check instead of a page check, kept in memory: a relationship is identified by a KEY, the unordered pair
 * of its endpoints, its type and the properties the statement matched it by, hashed into one of {@link #STRIPES} stripes. Each
 * stripe holds the sequence of the last commit that created a relationship hashing to it, and how many commits are creating one
 * right now. A transaction holding the sequence of its begin refuses to commit when a stripe it created a relationship in moved
 * since, or is being written by another commit, so its retry reads again and finds the relationship.
 * <p>
 * Two stripes per created relationship: the one of its own key, which the commit checks and marks, and the one of the same pair and
 * type with NO properties, which it only marks. A statement that matches by type alone finds any relationship of the type between
 * the pair, so it checks that second stripe and is refused by any concurrent commit of the pair and type, whatever properties that
 * one matched by. The reverse is not needed: a relationship created by type alone is not one a statement matching by properties
 * would have found, so committing both is the serial order in which the property statement runs second.
 * <p>
 * What it costs and what it trades:
 * <ul>
 * <li>a transaction that never matched-or-created a relationship pays one volatile read at begin, and nothing at commit;</li>
 * <li>a stripe shared by two keys (a hash collision) refuses a commit that did not conflict: a retry, never a wrong answer;</li>
 * <li>two transactions creating the same relationship at the same moment: the second to reach its commit is refused, never both,
 * because the check and the mark are one step under this registry's monitor;</li>
 * <li>a relationship whose identifying properties are a SUPERSET of the ones another statement matched it by, created by a
 * concurrent statement with the larger map, is not seen by the one with the smaller (non-empty) map. A unique constraint on the
 * identifying property is exact in every case, and remains the declaration to use where it applies.</li>
 * </ul>
 * The monitor guards only the two arrays: no I/O and no other lock is ever taken while holding it, which is what makes it safe to take
 * under the commit file locks without a lock-order inversion. Keep it that way.
 * <p>
 * In memory, per database and per server: a transaction originated on another member of a cluster commits through its own leader
 * and is checked there, where the conflicting commits are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MergedEdgeRegistry {
  /** A power of two: a stripe is the low bits of the key's hash. 16K stripes, 192KB per database, allocated on the first use. */
  public static final int STRIPES = 1 << 14;

  private final AtomicLong sequence = new AtomicLong();
  // Guarded by this. Allocated on the first commit that creates a relationship this way: most databases never do
  private       long[]     lastCommit;
  private       int[]      inFlight;

  /** The sequence a transaction takes at its begin: a relationship committed after it is one the transaction could not have read. */
  public long getSequence() {
    return sequence.get();
  }

  /**
   * Under the commit locks of the transaction, before its pages are validated: refuses it when a stripe it checks moved after
   * {@code since} or is being written by another commit, and otherwise marks every stripe it creates in as being written, until
   * {@link #committed} or {@link #release} is called with the same stripes.
   *
   * @param checked      the stripes of the relationships the transaction created, by the key they were matched with
   * @param checkedCount how many of {@code checked} are set
   * @param marked       the stripes the transaction's relationships will be found under once committed
   * @param markedCount  how many of {@code marked} are set
   * @param since        the sequence the transaction took at its begin
   *
   * @return true when the stripes are reserved, false when the transaction has to be retried
   */
  public synchronized boolean reserve(final int[] checked, final int checkedCount, final int[] marked, final int markedCount,
      final long since) {
    if (lastCommit == null) {
      lastCommit = new long[STRIPES];
      inFlight = new int[STRIPES];
    }

    // Every check before the first mark: two relationships of the same transaction can share a stripe, and the transaction is not
    // its own conflict
    for (int i = 0; i < checkedCount; i++) {
      final int stripe = checked[i];
      if (lastCommit[stripe] > since || inFlight[stripe] > 0)
        return false;
    }

    for (int i = 0; i < markedCount; i++)
      ++inFlight[marked[i]];
    return true;
  }

  /**
   * Called once the transaction's pages are published, before its file locks are released: the relationships it created are visible
   * to any read that starts from now on, and a transaction that began before is refused by the stripes moving past its sequence.
   */
  public synchronized void committed(final int[] marked, final int markedCount) {
    final long committedAt = sequence.incrementAndGet();
    for (int i = 0; i < markedCount; i++) {
      final int stripe = marked[i];
      lastCommit[stripe] = committedAt;
      unmark(stripe);
    }
  }

  /** Called when a transaction that reserved its stripes does not publish: what it marked is no longer being written. */
  public synchronized void release(final int[] marked, final int markedCount) {
    if (inFlight == null)
      return;
    for (int i = 0; i < markedCount; i++)
      unmark(marked[i]);
  }

  /** Never below zero: a stripe left marked by a double release would refuse every later commit in it, for the life of the database. */
  private void unmark(final int stripe) {
    if (inFlight[stripe] > 0)
      --inFlight[stripe];
  }

  /** How many commits are writing the stripe now, for the tests. */
  synchronized int inFlight(final int stripe) {
    return inFlight == null ? 0 : inFlight[stripe];
  }

  /**
   * The stripe of a relationship between two vertices: the pair is unordered, so the key of {@code (a)-[:T]->(b)} is the key of
   * {@code (b)-[:T]-(a)} too, which an undirected match finds whichever way the relationship points.
   *
   * @param properties the properties the relationship was matched by, null or empty for a match by type alone
   */
  public static int stripeOf(final RID a, final RID b, final String type, final Map<String, ?> properties) {
    final long first = pack(a);
    final long second = pack(b);
    long hash = mix(Math.min(first, second)) * 31 + mix(Math.max(first, second));
    hash = hash * 31 + (type != null ? type.hashCode() : 0);
    if (properties != null && !properties.isEmpty()) {
      // Order-independent: two maps holding the same entries hash alike whatever order they iterate in
      long entries = 0;
      for (final Map.Entry<String, ?> entry : properties.entrySet())
        entries += mix(entry.getKey().hashCode() * 0x9E3779B97F4A7C15L + valueHash(entry.getValue()));
      hash = hash * 31 + entries;
    }
    return (int) (mix(hash) & (STRIPES - 1));
  }

  private static long pack(final RID rid) {
    return ((long) rid.getBucketId() << 48) ^ rid.getPosition();
  }

  /**
   * A hash two values that compare equal in a match share: a whole number is hashed as a long whatever its class, so an INTEGER
   * property matched by a LONG parameter or a DOUBLE with no fraction is the same key. Two values that do not compare equal may
   * share it too, which only costs a retry.
   */
  private static long valueHash(final Object value) {
    if (value == null)
      return 0;
    if (value instanceof Number number) {
      final double asDouble = number.doubleValue();
      final long asLong = number.longValue();
      return asDouble == (double) asLong ? Long.hashCode(asLong) : Double.hashCode(asDouble);
    }
    if (value instanceof Map<?, ?> map) {
      long entries = 0;
      for (final Map.Entry<?, ?> entry : map.entrySet())
        entries += mix(valueHash(entry.getKey()) * 0x9E3779B97F4A7C15L + valueHash(entry.getValue()));
      return entries;
    }
    if (value instanceof Collection<?> collection) {
      long hash = 1;
      for (final Object element : collection)
        hash = hash * 31 + valueHash(element);
      return hash;
    }
    return value.hashCode();
  }

  /** The finalizer of SplitMix64: spreads the bits of a key over the whole word, so its low bits make a fair stripe. */
  private static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }
}
