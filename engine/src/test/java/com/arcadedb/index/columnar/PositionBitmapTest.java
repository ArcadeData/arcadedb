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
package com.arcadedb.index.columnar;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PositionBitmapTest {
  private static final int PAGE = 2048;

  @Test
  void setGetClearAcrossPages() {
    final PositionBitmap bitmap = new PositionBitmap(PAGE);
    bitmap.set(0);
    bitmap.set(2047);
    bitmap.set(2048);
    bitmap.set(5_000_000_000L);
    assertThat(bitmap.get(0)).isTrue();
    assertThat(bitmap.get(2047)).isTrue();
    assertThat(bitmap.get(2048)).isTrue();
    assertThat(bitmap.get(5_000_000_000L)).isTrue();
    assertThat(bitmap.get(1)).isFalse();
    assertThat(bitmap.get(2049)).isFalse();
    assertThat(bitmap.get(5_000_000_001L)).isFalse();
    bitmap.clear(2047);
    assertThat(bitmap.get(2047)).isFalse();
    assertThat(bitmap.cardinality()).isEqualTo(3);
    assertThat(bitmap.pages()).containsExactly(0, 1, 2441406);
  }

  @Test
  void wordBoundariesInsideAStripe() {
    final PositionBitmap bitmap = new PositionBitmap(PAGE);
    for (final long p : new long[] { 63, 64, 65, 127, 128 })
      bitmap.set(p);
    assertThat(bitmap.cardinality()).isEqualTo(5);
    bitmap.clear(64);
    assertThat(bitmap.get(63)).isTrue();
    assertThat(bitmap.get(64)).isFalse();
    assertThat(bitmap.get(65)).isTrue();
    final List<Long> seen = new ArrayList<>();
    bitmap.forEach(seen::add);
    assertThat(seen).containsExactly(63L, 65L, 127L, 128L);
  }

  @Test
  void externalBucketStripeIsFourWords() {
    final PositionBitmap bitmap = new PositionBitmap(256);
    bitmap.set(255);
    bitmap.set(256);
    assertThat(bitmap.pages()).containsExactly(0, 1);
    assertThat(bitmap.cardinality()).isEqualTo(2);
    assertThat(bitmap.stripeForTesting(0)).hasSize(4);
    assertThat(bitmap.maxRecordsInPage()).isEqualTo(256);
  }

  @Test
  void rejectsSlotCountNotMultipleOf64() {
    assertThatThrownBy(() -> new PositionBitmap(100)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new PositionBitmap(0)).isInstanceOf(IllegalArgumentException.class);
  }

  private static void fill(final Random random, final PositionBitmap bitmap, final BitSet set) {
    for (int i = 0; i < 1_500; i++) {
      final int p = random.nextInt(40) * PAGE + random.nextInt(PAGE / 8);
      bitmap.set(p);
      set.set(p);
    }
  }

  private static void assertSame(final PositionBitmap bitmap, final BitSet expected) {
    assertThat(bitmap.cardinality()).isEqualTo(expected.cardinality());
    final List<Long> got = new ArrayList<>();
    bitmap.forEach(got::add);
    final List<Long> want = new ArrayList<>();
    for (int i = expected.nextSetBit(0); i >= 0; i = expected.nextSetBit(i + 1))
      want.add((long) i);
    assertThat(got).isEqualTo(want);
  }

  @Test
  void booleanOpsMatchJavaBitSet() {
    final Random random = new Random(42);
    final PositionBitmap a = new PositionBitmap(PAGE);
    final PositionBitmap b = new PositionBitmap(PAGE);
    final BitSet sa = new BitSet();
    final BitSet sb = new BitSet();
    fill(random, a, sa);
    fill(random, b, sb);

    final BitSet andSet = (BitSet) sa.clone();
    andSet.and(sb);
    assertThat(a.andCardinality(b)).isEqualTo(andSet.cardinality());
    // the visiting form gives the same positions, ascending, without building the intersection
    final List<Long> visited = new ArrayList<>();
    a.forEachAnd(b, visited::add);
    final List<Long> expectedAnd = new ArrayList<>();
    andSet.stream().forEach(p -> expectedAnd.add((long) p));
    assertThat(visited).isEqualTo(expectedAnd);

    final PositionBitmap and = a.copy();
    and.and(b);
    assertSame(and, andSet);

    final BitSet andNotSet = (BitSet) sa.clone();
    andNotSet.andNot(sb);
    final PositionBitmap andNot = a.copy();
    andNot.andNot(b);
    assertSame(andNot, andNotSet);

    final BitSet orSet = (BitSet) sa.clone();
    orSet.or(sb);
    final PositionBitmap or = a.copy();
    or.or(b);
    assertSame(or, orSet);

    // operands untouched
    assertSame(a, sa);
    assertSame(b, sb);
  }

  @Test
  void orOfInterleavedPageSetsKeepsOrder() {
    final PositionBitmap a = new PositionBitmap(PAGE);
    final PositionBitmap b = new PositionBitmap(PAGE);
    for (int page = 0; page < 10; page += 2)
      a.set((long) page * PAGE + 3);
    for (int page = 1; page < 10; page += 2)
      b.set((long) page * PAGE + 4);
    a.or(b);
    assertThat(a.pages()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(a.cardinality()).isEqualTo(10);
    assertThat(b.pages()).containsExactly(1, 3, 5, 7, 9);
    // the stripes taken from b are clones: writing a must not reach b
    a.set(PAGE + 5);
    assertThat(b.get(PAGE + 5)).isFalse();
    assertThat(a.stripeForTesting(1)).isNotSameAs(b.stripeForTesting(1));
  }

  @Test
  void writesAfterOrWithSnapshotStayIsolated() {
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(1);
    b.set(PAGE + 1);
    final PositionBitmap s = b.snapshot();
    b.or(s);
    b.set(PAGE + 2);
    assertThat(s.get(PAGE + 2)).isFalse();
    s.set(2);
    assertThat(b.get(2)).isFalse();
    assertThat(b.stripeForTesting(1)).isNotSameAs(s.stripeForTesting(1));
  }

  @Test
  void booleanOpsWithDisjointAndEmptyOperands() {
    final PositionBitmap a = new PositionBitmap(PAGE);
    a.set(5);
    a.set(3 * PAGE + 7);
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(PAGE + 5);
    final PositionBitmap empty = new PositionBitmap(PAGE);

    assertThat(a.andCardinality(b)).isZero();
    assertThat(a.andCardinality(empty)).isZero();

    final PositionBitmap and = a.copy();
    and.and(b);
    assertThat(and.isEmpty()).isTrue();

    final PositionBitmap andEmpty = a.copy();
    andEmpty.and(empty);
    assertThat(andEmpty.isEmpty()).isTrue();

    final PositionBitmap andNot = a.copy();
    andNot.andNot(b);
    assertThat(andNot.cardinality()).isEqualTo(2);

    final PositionBitmap or = empty.copy();
    or.or(a);
    or.or(b);
    assertThat(or.pages()).containsExactly(0, 1, 3);
    assertThat(or.cardinality()).isEqualTo(3);
  }

  @Test
  void andNotThatEmptiesAStripeRemovesIt() {
    final PositionBitmap a = new PositionBitmap(PAGE);
    a.set(1);
    a.set(PAGE + 1);
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(1);
    a.andNot(b);
    assertThat(a.pages()).containsExactly(1);
  }

  @Test
  void emptiedStripeIsRemoved() {
    final PositionBitmap bitmap = new PositionBitmap(PAGE);
    assertThat(bitmap.isEmpty()).isTrue();
    bitmap.set(5);
    assertThat(bitmap.isEmpty()).isFalse();
    bitmap.clear(5);
    assertThat(bitmap.pages()).isEmpty();
    assertThat(bitmap.isEmpty()).isTrue();
    // clearing an absent bit in an absent page is a no-op
    bitmap.clear(99_999);
    assertThat(bitmap.isEmpty()).isTrue();
  }

  @Test
  void setPageAndClearPageCoverWholeStripe() {
    final PositionBitmap bitmap = new PositionBitmap(PAGE);
    bitmap.set(PAGE - 1);
    bitmap.setPage(1);
    bitmap.setPage(3);
    assertThat(bitmap.cardinality()).isEqualTo(1 + 2L * PAGE);
    assertThat(bitmap.get(PAGE)).isTrue();
    assertThat(bitmap.get(2L * PAGE - 1)).isTrue();
    assertThat(bitmap.get(2L * PAGE)).isFalse();
    bitmap.clearPage(1);
    bitmap.clearPage(2);
    assertThat(bitmap.pages()).containsExactly(0, 3);
    assertThat(bitmap.cardinality()).isEqualTo(1 + PAGE);
  }

  @Test
  void forEachIsAscending() {
    final Random random = new Random(7);
    final TreeSet<Long> expected = new TreeSet<>();
    final PositionBitmap bitmap = new PositionBitmap(PAGE);
    for (int i = 0; i < 3000; i++) {
      final long p = (long) random.nextInt(50) * PAGE + random.nextInt(PAGE);
      bitmap.set(p);
      expected.add(p);
    }
    final List<Long> got = new ArrayList<>();
    bitmap.forEach(got::add);
    assertThat(got).containsExactlyElementsOf(expected);
  }

  @Test
  void snapshotIsIsolatedFromLaterWrites() {
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(10);
    b.set(PAGE + 10);
    final PositionBitmap s = b.snapshot();
    b.set(11);
    b.set(5 * PAGE);
    b.clear(10);
    assertThat(s.get(11)).isFalse();
    assertThat(s.get(5L * PAGE)).isFalse();
    assertThat(s.get(10)).isTrue();
    assertThat(s.cardinality()).isEqualTo(2);

    // writes to the snapshot do not reach the source
    s.set(12);
    s.clear(PAGE + 10);
    assertThat(b.get(12)).isFalse();
    assertThat(b.get(PAGE + 10)).isTrue();

    // a second generation of snapshots stays isolated too
    final PositionBitmap s2 = b.snapshot();
    b.set(13);
    s2.set(14);
    assertThat(s2.get(13)).isFalse();
    assertThat(b.get(14)).isFalse();
    assertThat(s.get(13)).isFalse();
  }

  @Test
  void snapshotIsIsolatedFromBooleanOps() {
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(1);
    b.set(PAGE + 1);
    final PositionBitmap s = b.snapshot();
    final PositionBitmap mask = new PositionBitmap(PAGE);
    mask.set(1);
    b.and(mask);
    assertThat(b.cardinality()).isEqualTo(1);
    assertThat(s.cardinality()).isEqualTo(2);
    b.or(s);
    s.andNot(mask);
    assertThat(b.cardinality()).isEqualTo(2);
    assertThat(s.cardinality()).isEqualTo(1);
  }

  @Test
  void snapshotSharesUntouchedStripes() {
    final PositionBitmap b = new PositionBitmap(PAGE);
    for (int page = 0; page < 6; page++)
      b.set((long) page * PAGE + 1);
    final PositionBitmap s = b.snapshot();
    b.set(3L * PAGE + 2);
    for (int page = 0; page < 6; page++) {
      if (page == 3)
        assertThat(b.stripeForTesting(page)).isNotSameAs(s.stripeForTesting(page));
      else
        assertThat(b.stripeForTesting(page)).isSameAs(s.stripeForTesting(page));
    }
  }

  @Test
  void copyIsDeepAndIndependent() {
    final PositionBitmap b = new PositionBitmap(PAGE);
    b.set(1);
    b.set(PAGE + 1);
    final PositionBitmap c = b.copy();
    assertThat(c.stripeForTesting(0)).isNotSameAs(b.stripeForTesting(0));
    assertThat(c.stripeForTesting(1)).isNotSameAs(b.stripeForTesting(1));
    assertThat(c.stripeForTesting(0)).isEqualTo(b.stripeForTesting(0));
    c.set(2);
    assertThat(b.get(2)).isFalse();
    b.set(3);
    assertThat(c.get(3)).isFalse();
  }

  @Test
  void mismatchedSlotCountsAreRefused() {
    final PositionBitmap a = new PositionBitmap(PAGE);
    final PositionBitmap other = new PositionBitmap(256);
    assertThatThrownBy(() -> a.and(other)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> a.andNot(other)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> a.or(other)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> a.andCardinality(other)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void orStripeOrsWordsAndNeverCreatesAnEmptyStripe() {
    final PositionBitmap bitmap = new PositionBitmap(128);
    final long[] words = { 0, 0, 0b101L, 1L << 63 };
    bitmap.orStripe(4, words, 0);
    assertThat(bitmap.pages()).isEmpty();
    bitmap.orStripe(4, words, 2);
    bitmap.orStripe(4, new long[] { 0b10L, 0 }, 0);
    assertThat(bitmap.pages()).containsExactly(4);
    assertThat(bitmap.get(4 * 128)).isTrue();
    assertThat(bitmap.get(4 * 128 + 1)).isTrue();
    assertThat(bitmap.get(4 * 128 + 2)).isTrue();
    assertThat(bitmap.get(4 * 128 + 127)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(4);
    assertThat(bitmap.stripe(4)).containsExactly(0b111L, 1L << 63);
    assertThat(bitmap.stripe(5)).isNull();

    // a stripe shared with a snapshot is cloned before the OR
    final PositionBitmap snapshot = bitmap.snapshot();
    bitmap.orStripe(4, new long[] { 0b1000L, 0 }, 0);
    assertThat(snapshot.get(4 * 128 + 3)).isFalse();
    assertThat(bitmap.get(4 * 128 + 3)).isTrue();
  }

  /** forEachAndNotAny visits what andNot over the union of the others leaves, against a BitSet oracle, without changing any bitmap. */
  @Test
  void forEachAndNotAnyMatchesAndNotOverTheUnion() {
    final Random random = new Random(11);
    final PositionBitmap base = new PositionBitmap(PAGE);
    final PositionBitmap[] others = { new PositionBitmap(PAGE), new PositionBitmap(PAGE), new PositionBitmap(PAGE) };
    final BitSet expected = new BitSet();
    for (int i = 0; i < 20_000; i++) {
      // pages 0 to 9; the others leave some pages untouched, and cover word boundaries
      final int position = random.nextInt(10 * PAGE);
      base.set(position);
      expected.set(position);
      if (random.nextInt(3) == 0)
        others[random.nextInt(others.length)].set(random.nextInt(8 * PAGE));
    }
    for (final PositionBitmap other : others)
      other.forEach(position -> expected.clear((int) position));
    final long baseCardinality = base.cardinality();

    final List<Long> seen = new ArrayList<>();
    base.forEachAndNotAny(others, seen::add);
    final List<Long> oracle = new ArrayList<>();
    expected.stream().forEach(position -> oracle.add((long) position));
    assertThat(seen).isEqualTo(oracle);
    assertThat(base.cardinality()).isEqualTo(baseCardinality);

    final List<Long> none = new ArrayList<>();
    base.forEachAndNotAny(new PositionBitmap[0], none::add);
    assertThat(none).hasSize((int) baseCardinality);
    assertThatThrownBy(() -> base.forEachAndNotAny(new PositionBitmap[] { new PositionBitmap(256) }, p -> {
    })).isInstanceOf(IllegalArgumentException.class);
  }
}
