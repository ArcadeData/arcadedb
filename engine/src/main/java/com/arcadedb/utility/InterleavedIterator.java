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

  /**
   * Takes ownership of the given array (it is not copied): the caller must not touch it afterwards. The sources
   * are used from where they currently are - the constructor deliberately does NOT rewind them, so a partially
   * consumed source keeps its position; {@link #reset()} is what rewinds.
   */
  @SuppressWarnings("unchecked")
  public InterleavedIterator(final Iterator<T>[] sources) {
    this.sources = sources;
    this.active = new Iterator[sources.length];
    System.arraycopy(sources, 0, active, 0, sources.length);
    this.activeCount = sources.length;
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
    }

    return false;
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final T value = active[pending].next();
    pending = -1;
    ++turn;
    ++browsed;
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
