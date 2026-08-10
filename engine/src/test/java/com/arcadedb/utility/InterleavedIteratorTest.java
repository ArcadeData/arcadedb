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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #6044: round-robin composition of several iterators, used by {@code StripedEdgeList} to keep an approximate
 * newest-first order across the stripe chains of a promoted super-node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InterleavedIteratorTest {

  @Test
  void rotatesOneEntryPerSource() {
    assertThat(drain(of("a1", "a2", "a3"), of("b1", "b2", "b3"), of("c1", "c2", "c3")))
        .containsExactly("a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3");
  }

  @Test
  void dropsExhaustedSourcesAndKeepsRotatingTheRest() {
    // The short source leaves the rotation after its only entry; the others must not skip a turn or repeat one.
    assertThat(drain(of("a1"), of("b1", "b2", "b3"), of("c1", "c2")))
        .containsExactly("a1", "b1", "c1", "b2", "c2", "b3");
  }

  @Test
  void toleratesEmptySourcesInAnyPosition() {
    assertThat(drain(of(), of("b1", "b2"), of(), of("d1"), of()))
        .containsExactly("b1", "d1", "b2");
    assertThat(drain(of(), of(), of())).isEmpty();
  }

  @Test
  void singleSourceIsPassedThroughInOrder() {
    assertThat(drain(of("a1", "a2", "a3"))).containsExactly("a1", "a2", "a3");
  }

  @Test
  void noSourceAtAllIsAnEmptyIteration() {
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources());
    assertThat(it.hasNext()).isFalse();
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void repeatedHasNextDoesNotConsumeOrAdvanceTheRotation() {
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources(of("a1", "a2"), of("b1", "b2")));
    for (int i = 0; i < 5; i++)
      assertThat(it.hasNext()).isTrue();
    assertThat(it.next()).isEqualTo("a1");
    assertThat(it.hasNext()).isTrue();
    assertThat(it.hasNext()).isTrue();
    assertThat(it.next()).isEqualTo("b1");
    assertThat(drainRest(it)).containsExactly("a2", "b2");
  }

  @Test
  void nextWithoutHasNextStillThrowsAtTheEnd() {
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources(of("a1")));
    assertThat(it.next()).isEqualTo("a1");
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void browsedCountsTheEntriesActuallyEmitted() {
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources(of("a1", "a2"), of("b1")));
    assertThat(it.getBrowsed()).isZero();
    it.next();
    it.next();
    assertThat(it.getBrowsed()).isEqualTo(2);
    drainRest(it);
    assertThat(it.getBrowsed()).isEqualTo(3);
  }

  @Test
  void countEntriesIsTheTotalRegardlessOfHowFarTheRotationGot() {
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources(of("a1", "a2"), of("b1"), of()));
    assertThat(it.countEntries()).isEqualTo(3);
  }

  @Test
  void resetReplaysEveryResettableSource() {
    final ResettableList<String> a = new ResettableList<>(List.of("a1", "a2"));
    final ResettableList<String> b = new ResettableList<>(List.of("b1", "b2", "b3"));
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources(a, b));

    assertThat(drainRest(it)).containsExactly("a1", "b1", "a2", "b2", "b3");
    it.reset();
    assertThat(it.getBrowsed()).isZero();
    assertThat(drainRest(it)).containsExactly("a1", "b1", "a2", "b2", "b3");
  }

  /**
   * The property the striped edge list depends on: with N similarly-ordered sources, an entry of global rank
   * {@code r} comes back at a position of order {@code r}, not of order (rank x source length).
   */
  @Test
  void positionTracksRankNotSourceLength() {
    final int sourceCount = 16;
    final int perSource = 500;
    final List<String> ranked = new ArrayList<>(sourceCount * perSource);
    for (int r = 0; r < sourceCount * perSource; r++)
      ranked.add("r" + r);

    // Deal the globally-ordered entries round-robin into the sources, so each source is itself ordered.
    final List<List<String>> buckets = new ArrayList<>(sourceCount);
    for (int s = 0; s < sourceCount; s++)
      buckets.add(new ArrayList<>(perSource));
    for (int r = 0; r < ranked.size(); r++)
      buckets.get(r % sourceCount).add(ranked.get(r));

    @SuppressWarnings("unchecked") final Iterator<String>[] cursors = new Iterator[sourceCount];
    for (int s = 0; s < sourceCount; s++)
      cursors[s] = buckets.get(s).iterator();

    final List<String> out = drainRest(new InterleavedIterator<>(cursors));
    assertThat(out).hasSize(ranked.size());
    // Perfectly round-robin input round-trips exactly; the real hash only perturbs it locally.
    assertThat(out).isEqualTo(ranked);
  }

  private static Iterator<String> of(final String... values) {
    return List.of(values).iterator();
  }

  @SafeVarargs
  private static Iterator<String>[] sources(final Iterator<String>... iterators) {
    return iterators;
  }

  @SafeVarargs
  private static List<String> drain(final Iterator<String>... iterators) {
    return drainRest(new InterleavedIterator<>(iterators));
  }

  private static <T> List<T> drainRest(final Iterator<T> it) {
    final List<T> out = new ArrayList<>();
    while (it.hasNext())
      out.add(it.next());
    return out;
  }

  /** Minimal {@link ResettableIterator} over a fixed list, to exercise the reset/countEntries delegation. */
  private static class ResettableList<T> implements ResettableIterator<T> {
    private final List<T> values;
    private       int     position;
    private       long    browsed;

    ResettableList(final List<T> values) {
      this.values = values;
    }

    @Override
    public boolean hasNext() {
      return position < values.size();
    }

    @Override
    public T next() {
      if (!hasNext())
        throw new NoSuchElementException();
      ++browsed;
      return values.get(position++);
    }

    @Override
    public void reset() {
      position = 0;
      browsed = 0;
    }

    @Override
    public long countEntries() {
      return values.size();
    }

    @Override
    public long getBrowsed() {
      return browsed;
    }
  }
}
