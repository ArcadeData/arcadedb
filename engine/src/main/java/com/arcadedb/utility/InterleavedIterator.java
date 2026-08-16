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
package com.arcadedb.utility;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Round-robin composition of several iterators: emits one entry from each source in turn, dropping a source
 * from the rotation as soon as it is exhausted, until every source is drained. Unlike {@link MultiIterator},
 * which CONCATENATES its sources, this preserves the relative rank of an entry across sources that are each
 * ordered the same way.
 * <p>
 * It exists for the striped super-node edge list ({@code StripedEdgeList}, #6044), where the logical edge list
 * is split over N chains by {@code hash(neighbour RID)} and each chain is newest-first: concatenating them makes
 * the global order a function of the hash, with an error proportional to the vertex DEGREE, while interleaving
 * them reconstructs an approximate newest-first order whose error is proportional to the RANK asked for. It is
 * not tied to graphs though - any set of similarly-ordered sources merges the same way.
 * <p>
 * The rotation is kept in a plain array that is compacted on exhaustion, so steady-state cost per entry is one
 * array read plus one bounds check; no per-entry allocation.
 * <h2>Widening the rotation past a threshold (#6048, #6064)</h2>
 * Taking ONE entry per turn keeps every source's current position resident (one chunk page per source, in the
 * super-node case) and hops between sources on every entry. That is worth paying while the rank of the entries
 * still matters to the caller, and it is pure locality cost once it does not - which is the position a full walk
 * is in for all but its first entries.
 * <p>
 * Past the {@code degradeAfter}-th entry the rotation therefore takes a BATCH of {@code degradeBatch} entries
 * from each source per turn instead of one, and doubles that batch on every completed round. The two properties
 * that matters are kept at once:
 * <ul>
 * <li><b>locality</b>: the batch grows geometrically, so the number of source switches over a walk of {@code D}
 *     entries is {@code O(sources x log(D))} rather than {@code O(D)}, and once the batch exceeds a source's own
 *     chunking every visit is a sequential run - the #6048 cost, still bounded;</li>
 * <li><b>rank fidelity</b>: an entry at depth {@code d} in its source is emitted no later than the end of the
 *     batch that reaches depth {@code d}, which is at most twice {@code d}, so its position stays within a
 *     constant factor of its true rank for the WHOLE walk (#6064). Freezing the rotation instead - draining one
 *     source to exhaustion and then the next - loses that past the threshold, with an error proportional to the
 *     total number of entries, which is what a caller with a large-but-bounded LIMIT was silently paying.</li>
 * </ul>
 * A {@code degradeBatch} of {@link Long#MAX_VALUE} is the pre-#6064 behaviour: the source holding the turn is
 * drained to exhaustion (the {@link #hasNext()} compaction slides the next source into the same slot when that
 * happens) and the remaining sources are concatenated from wherever the rotation left off.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InterleavedIterator<T> implements ResettableIterator<T> {
  private final Iterator<T>[] sources;
  /** The sources still in the rotation, compacted: {@code active[0..activeCount)}. */
  private final Iterator<T>[] active;
  private       int           activeCount;
  /** Index in {@code active} of the source whose turn it is. */
  private       int           turn;
  /** Index in {@code active} of the source holding the already-peeked entry, or -1 when none is pending. */
  private       int           pending = -1;
  private       long          browsed = 0L;
  /** Entry count at which the rotation widens. {@code Long.MAX_VALUE} keeps it one-entry-per-turn forever. */
  private final long          degradeAfter;
  /** The value {@link #batch} takes when the threshold is crossed; it doubles from there, this does not. */
  private final long          degradeBatch;
  /** Entries to take from the source holding the turn before rotating: 1 until the threshold, then widening. */
  private       long          batch;
  /** Entries already taken from the source holding the turn in this visit. */
  private       long          taken;
  /**
   * Rotations since the last doubling of {@link #batch}; a full round is {@code activeCount} of them.
   * <p>
   * Deliberately counts only the visits that ran their batch out, not the ones cut short by a source going
   * exhausted - those drop the source, so they shrink {@link #activeCount}, which is the very bound this is
   * compared against. The doubling cadence is therefore approximate when the sources are uneven in length, and
   * that is the intended reading: what this class promises is a BOUND on the batch's growth (and so on the
   * switch count and the rank error), never an exact round length. Counting a dropped source as a rotation
   * would make the cadence no more exact and would double the batch for the survivors on the strength of a
   * visit that never happened.
   */
  private       int           rotationsInRound;
  /** True once {@link #browsed} has reached {@link #degradeAfter}, i.e. once {@link #batch} may grow. */
  private       boolean       widening;

  /**
   * Takes ownership of the given array (it is not copied): the caller must not touch it afterwards. The sources
   * are used from where they currently are - the constructor deliberately does NOT rewind them, so a partially
   * consumed source keeps its position; {@link #reset()} is what rewinds.
   * <p>
   * Stays one entry per turn for the whole walk; equivalent to {@code InterleavedIterator(sources, Long.MAX_VALUE)}.
   */
  public InterleavedIterator(final Iterator<T>[] sources) {
    this(sources, Long.MAX_VALUE);
  }

  /**
   * As {@link #InterleavedIterator(Iterator[])}, but the rotation degrades to plain concatenation of the
   * remaining sources once {@code degradeAfter} entries have been emitted.
   *
   * @param degradeAfter entry count at which to degrade, or {@code Long.MAX_VALUE} to never degrade. Not
   *                      validated to be positive: a value {@code <= 0} degrades from the very first entry,
   *                      which is a legitimate (if unusual) way to ask for plain concatenation through this class.
   */
  public InterleavedIterator(final Iterator<T>[] sources, final long degradeAfter) {
    this(sources, degradeAfter, Long.MAX_VALUE);
  }

  /**
   * As {@link #InterleavedIterator(Iterator[], long)}, but past the threshold the rotation WIDENS instead of
   * stopping: it keeps turning, taking {@code degradeBatch} entries from each source per turn and doubling that
   * batch on every completed round (see the class Javadoc).
   *
   * @param degradeAfter entry count at which the rotation widens, or {@code Long.MAX_VALUE} to never widen. A
   *                      value {@code <= 0} widens from the very first entry.
   * @param degradeBatch entries taken from one source per turn at that point, at least 1. {@code Long.MAX_VALUE}
   *                      is the degenerate case that never rotates again: plain concatenation of the tails.
   */
  @SuppressWarnings("unchecked")
  public InterleavedIterator(final Iterator<T>[] sources, final long degradeAfter, final long degradeBatch) {
    this.sources = sources;
    this.active = new Iterator[sources.length];
    System.arraycopy(sources, 0, active, 0, sources.length);
    this.activeCount = sources.length;
    this.degradeAfter = degradeAfter;
    this.degradeBatch = Math.max(1L, degradeBatch);
    armWidening();
  }

  /**
   * The starting state of the widening, shared by the constructor and {@link #reset()}: one entry per turn until
   * the threshold. A NON-POSITIVE threshold is never reached by {@link #browsed} (which starts at 1), so it arms
   * the widening straight away instead - which is how "degrade from the very first entry" is expressed.
   */
  private void armWidening() {
    widening = degradeAfter <= 0;
    batch = widening ? degradeBatch : 1L;
    taken = 0L;
    rotationsInRound = 0;
  }

  @Override
  public boolean hasNext() {
    if (pending > -1)
      return true;

    // Each turn of this loop either finds the next entry or removes one exhausted source, so it terminates.
    while (activeCount > 0) {
      if (turn >= activeCount)
        turn = 0;

      if (active[turn].hasNext()) {
        pending = turn;
        return true;
      }

      // EXHAUSTED: DROP IT FROM THE ROTATION. The compaction slides the following source into this slot, so
      // `turn` already points at it and must not be advanced.
      System.arraycopy(active, turn + 1, active, turn, activeCount - turn - 1);
      active[--activeCount] = null;
      // The batch quota belonged to the source that is gone: the one sliding into its slot starts a fresh visit.
      taken = 0L;
    }

    return false;
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final T value = active[pending].next();
    pending = -1;
    ++browsed;

    // Crossing the threshold arms the widening and starts the first wide visit on the source holding the turn.
    // The quota is RE-BASED, not extended: this entry becomes the 1st of that source's `degradeBatch`-long visit
    // rather than being counted both as the last one-per-turn entry and as the first wide one, so every source's
    // first wide visit is the same length whichever of them happened to hold the turn at the crossing.
    if (browsed == degradeAfter) {
      widening = true;
      batch = degradeBatch;
      taken = 0L;
    }

    if (++taken >= batch) {
      // Visit over: hand the turn to the next source. Below the threshold `batch` is 1, so this is the plain
      // one-entry-per-turn rotation; past it the visits get geometrically longer (see the class Javadoc).
      taken = 0L;
      ++turn;
      if (widening && ++rotationsInRound >= activeCount) {
        rotationsInRound = 0;
        batch = batch <= (Long.MAX_VALUE >> 1) ? batch << 1 : Long.MAX_VALUE;
      }
    }
    return value;
  }

  /**
   * Rewinds the rotation and every source that can be rewound. A source that is a plain {@link Iterator} cannot
   * be, so it silently keeps its position and this iterator replays only what is left of it - the same limitation
   * {@link MultiIterator#reset()} has. Every source the engine passes here is a {@code ResettableIterator}
   * ({@code EdgeIterator}, {@code VertexIterator}, {@code RIDIterator} and their filtering variants all extend
   * {@code ResettableIteratorBase}), so a full replay is what actually happens on every real call site.
   */
  @Override
  public void reset() {
    for (final Iterator<T> source : sources)
      if (source instanceof ResettableIterator<?> resettable)
        resettable.reset();

    System.arraycopy(sources, 0, active, 0, sources.length);
    activeCount = sources.length;
    turn = 0;
    pending = -1;
    browsed = 0L;
    armWidening();
  }

  /**
   * Total entries across every source, whether or not the rotation has already consumed them - the same
   * "size of the whole thing" semantics {@link MultiIterator#countEntries()} has, and it is summed from the
   * sources rather than from the rotation so an exhausted-and-dropped source still counts.
   * <p>
   * A {@link ResettableIterator} source counts without losing its place. A plain {@link Iterator} one cannot be
   * counted any other way than by draining it, exactly as {@code MultiIterator} drains a non-resettable source -
   * so calling this over plain iterators consumes them.
   */
  @Override
  public long countEntries() {
    long total = 0L;
    for (final Iterator<T> source : sources)
      if (source instanceof ResettableIterator<?> resettable)
        total += resettable.countEntries();
      else
        total += CollectionUtils.countEntries(source);
    return total;
  }

  @Override
  public long getBrowsed() {
    return browsed;
  }
}
