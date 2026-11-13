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

import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IteratorTest {

  @Test
  void iterableObjectWithSingleValue() {
    final IterableObject<String> iterable = new IterableObject<>("hello");

    final List<String> result = new ArrayList<>();
    iterable.forEach(result::add);

    assertThat(result).containsExactly("hello");
  }

  @Test
  void iterableObjectIterator() {
    final IterableObject<Integer> iterable = new IterableObject<>(42);
    final Iterator<Integer> iterator = iterable.iterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(42);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void iterableObjectIteratorThrowsOnExhausted() {
    final IterableObject<String> iterable = new IterableObject<>("value");
    final Iterator<String> iterator = iterable.iterator();

    iterator.next(); // Consume the single element

    assertThatThrownBy(iterator::next)
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void iterableObjectArrayWithObjectArray() {
    final String[] array = {"a", "b", "c"};
    final IterableObjectArray<String> iterable = new IterableObjectArray<>(array);

    final List<String> result = new ArrayList<>();
    for (final String s : iterable) {
      result.add(s);
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void iterableObjectArrayWithIntArray() {
    final int[] array = {1, 2, 3};
    final IterableObjectArray<Integer> iterable = new IterableObjectArray<>(array);

    final List<Integer> result = new ArrayList<>();
    for (final Integer i : iterable) {
      result.add(i);
    }

    assertThat(result).containsExactly(1, 2, 3);
  }

  @Test
  void iterableObjectArrayIterator() {
    final Double[] array = {1.0, 2.0};
    final IterableObjectArray<Double> iterable = new IterableObjectArray<>(array);
    final Iterator<Double> iterator = iterable.iterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(1.0);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(2.0);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void multiIteratorWithSingleSource() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void multiIteratorWithMultipleSources() {
    final MultiIterator<Integer> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList(1, 2));
    multi.addIterator(Arrays.asList(3, 4));
    multi.addIterator(Arrays.asList(5, 6));

    final List<Integer> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void multiIteratorWithSkip() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setSkip(2);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("c", "d", "e");
  }

  @Test
  void multiIteratorWithLimit() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setLimit(3);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void multiIteratorWithSkipAndLimit() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setSkip(1);
    multi.setLimit(2);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("b", "c");
  }

  @Test
  void multiIteratorCountEntries() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b"));
    multi.addIterator(Arrays.asList("c", "d", "e"));

    assertThat(multi.countEntries()).isEqualTo(5);
  }

  @Test
  void multiIteratorCountEntriesWithNonResettableIterable() {
    // Regression test for #4396: countEntries() was discarding the count
    // returned by CollectionUtils.countEntries(iter) for non-resettable iterables.
    final MultiIterator<String> multi = new MultiIterator<>();

    // Plain Iterable backed by a non-resettable iterator (not a Collection or ResettableIterator)
    final Iterable<String> nonResettable = () -> Arrays.asList("x", "y", "z").iterator();
    multi.addIterator(nonResettable);
    multi.addIterator(Arrays.asList("a", "b"));

    assertThat(multi.countEntries()).isEqualTo(5);
  }

  @Test
  void multiIteratorGetBrowsed() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    multi.next();
    assertThat(multi.getBrowsed()).isEqualTo(1);

    multi.next();
    assertThat(multi.getBrowsed()).isEqualTo(2);
  }

  @Test
  void multiIteratorWithIteratorSource() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b").iterator());
    multi.addIterator(Arrays.asList("c", "d").iterator());

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c", "d");
  }

  @Test
  void multiIteratorReset() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    // Consume all
    while (multi.hasNext()) {
      multi.next();
    }

    multi.reset();

    // Should be able to iterate again
    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void lruCacheBasicOperations() {
    final LRUCache<String, Integer> cache = new LRUCache<>(4);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertThat(cache.get("a")).isEqualTo(1);
    assertThat(cache.get("b")).isEqualTo(2);
    assertThat(cache.get("c")).isEqualTo(3);
  }

  @Test
  void lruCacheEviction() {
    // LRUCache with size 3 holds 3 entries; the eldest is evicted only once
    // the capacity is exceeded
    final LRUCache<String, Integer> cache = new LRUCache<>(3);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertThat(cache.keySet()).containsExactlyInAnyOrder("a", "b", "c");

    // Adding "d" exceeds the capacity, so the eldest ("a") is evicted
    cache.put("d", 4);

    assertThat(cache.containsKey("a")).isFalse();
    assertThat(cache.containsKey("b")).isTrue();
    assertThat(cache.containsKey("c")).isTrue();
    assertThat(cache.containsKey("d")).isTrue();
  }

  @Test
  void lruCacheAccessOrder() {
    // LinkedHashMap with accessOrder=true reorders on get
    final LRUCache<String, Integer> cache = new LRUCache<>(3);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    // Access "a" to make it more recently used
    cache.get("a");

    // Adding "d" evicts "b" (the oldest in access order)
    cache.put("d", 4);

    assertThat(cache.containsKey("a")).isTrue();
    assertThat(cache.containsKey("b")).isFalse();
    assertThat(cache.containsKey("c")).isTrue();
    assertThat(cache.containsKey("d")).isTrue();
  }

  /**
   * Regression for #10377: {@link MultiIterator} and {@link InterleavedIterator} are what a promoted super-node serves every edge list
   * walk through, and both refused {@code remove()}. They now hand it to the source that produced the last element.
   */
  @Nested
  class CompositeIteratorRemove {

    @Test
    void multiIteratorRemovesFromTheSourceOfTheLastElement() {
      final List<String> a = new ArrayList<>(List.of("a1", "a2"));
      final List<String> b = new ArrayList<>(List.of("b1", "b2"));
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(a.iterator());
      it.addIterator(b.iterator());

      final List<String> seen = new ArrayList<>();
      while (it.hasNext()) {
        final String value = it.next();
        seen.add(value);
        if (value.equals("a2") || value.equals("b1"))
          it.remove();
      }

      assertThat(seen).containsExactly("a1", "a2", "b1", "b2");
      assertThat(a).containsExactly("a1");
      assertThat(b).containsExactly("b2");
    }

    @Test
    void multiIteratorRemoveAfterAHasNextThatMovedToTheNextSourceStillRemovesTheReturnedElement() {
      final List<String> a = new ArrayList<>(List.of("a1"));
      final List<String> b = new ArrayList<>(List.of("b1"));
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(a.iterator());
      it.addIterator(b.iterator());

      assertThat(it.next()).isEqualTo("a1");
      // The peek moves the cursor onto the second source: the removal is still about "a1"
      assertThat(it.hasNext()).isTrue();
      it.remove();

      assertThat(a).isEmpty();
      assertThat(b).containsExactly("b1");
      assertThat(it.next()).isEqualTo("b1");
    }

    @Test
    void multiIteratorRemoveBeforeNextIsIllegalState() {
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(new ArrayList<>(List.of("a1")).iterator());
      assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void multiIteratorSecondRemoveForTheSameElementIsIllegalState() {
      final List<String> a = new ArrayList<>(List.of("a1", "a2"));
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(a.iterator());
      it.next();
      it.remove();
      assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
      assertThat(a).containsExactly("a2");
    }

    @Test
    void interleavedIteratorSecondRemoveForTheSameEntryIsIllegalState() {
      final List<String> a = new ArrayList<>(List.of("a1", "a2"));
      @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator() };
      final InterleavedIterator<String> it = new InterleavedIterator<>(sources);
      it.next();
      it.remove();
      assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
      assertThat(a).containsExactly("a2");
    }

    @Test
    void multiIteratorRemoveAfterResetIsIllegalState() {
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(new ArrayList<>(List.of("a1", "a2")));
      it.next();
      it.reset();
      assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void multiIteratorKeepsTheSourceWhenItRefusesTheRemoval() {
      final Iterator<String> refusing = List.of("a1").iterator();
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(refusing);
      it.next();
      assertThatThrownBy(it::remove).isInstanceOf(UnsupportedOperationException.class);
      // A caller that knows the source better (CHECK DATABASE's keep-the-edge prune) can still reach it
      assertThat(it.getLastReturnedSource()).isSameAs(refusing);
    }

    @Test
    void multiIteratorOverACollectionSourceRemovesFromIt() {
      final List<String> a = new ArrayList<>(List.of("a1", "a2"));
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(a);
      it.next();
      it.remove();
      assertThat(a).containsExactly("a2");
    }

    @Test
    void interleavedIteratorRemovesFromTheSourceOfTheLastElement() {
      final List<String> a = new ArrayList<>(List.of("a1", "a2"));
      final List<String> b = new ArrayList<>(List.of("b1", "b2"));
      @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator(), b.iterator() };
      final InterleavedIterator<String> it = new InterleavedIterator<>(sources);

      final List<String> seen = new ArrayList<>();
      while (it.hasNext()) {
        final String value = it.next();
        seen.add(value);
        if (value.equals("b1") || value.equals("a2"))
          it.remove();
      }

      assertThat(seen).containsExactly("a1", "b1", "a2", "b2");
      assertThat(a).containsExactly("a1");
      assertThat(b).containsExactly("b2");
    }

    @Test
    void interleavedIteratorRemoveBeforeNextIsIllegalState() {
      @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { new ArrayList<>(List.of("a1")).iterator() };
      final InterleavedIterator<String> it = new InterleavedIterator<>(sources);
      assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nestedInterleavedInsideMultiRemoves() {
      final List<String> a = new ArrayList<>(List.of("a1"));
      final List<String> b = new ArrayList<>(List.of("b1"));
      @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator(), b.iterator() };
      final MultiIterator<String> it = new MultiIterator<>();
      it.addIterator(new InterleavedIterator<>(sources));

      assertThat(it.next()).isEqualTo("a1");
      assertThat(it.next()).isEqualTo("b1");
      it.remove();
      assertThat(a).containsExactly("a1");
      assertThat(b).isEmpty();
    }
  }

  /**
   * Regression for #6880.
   * <p>
   * https://github.com/ArcadeData/arcadedb/issues/6880
   * <p>
   * {@code MultiIterator.next()} used to re-run {@code hasNext()}, and {@code hasNextInternal()} consults the deadline.
   * A deadline that expired *between* the caller's {@code hasNext()} and its {@code next()} therefore turned into a bare
   * {@link NoSuchElementException} instead of the clean truncation {@code timeout(v, unit, false)} promises. Every
   * consumer driving a {@code MultiIterator} with a non-throwing deadline is exposed - {@code SelectExecutor.executeExists()},
   * {@code executeCount()} and {@code SelectIterator.fetchNext()} all have the same
   * {@code hasNext()} / {@code checkForTimeout()} / {@code next()} shape - which is why it only ever showed up under load.
   * <p>
   * The expiry decision is now made once per element rather than twice: an element {@code hasNext()} has already
   * promised stays deliverable, and the deadline stops the iteration on the *following* {@code hasNext()}.
   * <p>
   * These tests plant the expiry deterministically (the deadline is put in the past between the two calls) rather than
   * racing a 1 ms budget, which is what made the original symptom a 1-in-12 flake.
   */
  @Nested
  class MultiIteratorTimeout {

    /**
     * The statement of the bug: a non-throwing deadline that expires after {@code hasNext()} answered {@code true} must
     * not turn the promised element into a {@link NoSuchElementException}.
     */
    @Test
    void nonThrowingExpiryAfterHasNextStillDeliversThePromisedElement() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());

      assertThat(it.hasNext()).isTrue();
      expireDeadline(it, false);

      assertThatNoException().isThrownBy(it::next);
    }

    /**
     * ...and the truncation itself still happens, one element later: the promised element is the last one.
     */
    @Test
    void nonThrowingExpiryTruncatesOnTheFollowingHasNext() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());

      assertThat(it.hasNext()).isTrue();
      expireDeadline(it, false);

      assertThat(it.next()).isEqualTo(1);
      assertThat(it.hasNext()).isFalse();
    }

    /**
     * The throwing mode keeps reporting the expiry - also one element later, and still as a {@link TimeoutException},
     * never as a {@link NoSuchElementException}.
     */
    @Test
    void throwingExpiryAfterHasNextDeliversThenThrowsTimeout() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());

      assertThat(it.hasNext()).isTrue();
      expireDeadline(it, true);

      assertThat(it.next()).isEqualTo(1);
      assertThatThrownBy(it::hasNext).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    /**
     * An expiry that lands BEFORE any {@code hasNext()} promise must still stop the iteration immediately - the promise
     * is per element, it does not survive as a general licence to keep going.
     */
    @Test
    void expiryBeforeAnyPromiseStopsImmediately() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());
      expireDeadline(it, false);

      assertThat(it.hasNext()).isFalse();
      assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    }

    /**
     * A promise consumed by {@code next()} is spent: a second expiry check happens on the next element, so a repeated
     * {@code next()} without an intervening {@code hasNext()} cannot walk past the deadline either.
     */
    @Test
    void aPromiseIsSpentByTheNextThatConsumesIt() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());

      assertThat(it.hasNext()).isTrue();
      expireDeadline(it, false);

      assertThat(it.next()).isEqualTo(1);
      assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    }

    /**
     * {@code reset()} drops any outstanding promise along with the rest of the iteration state.
     */
    @Test
    void resetDropsTheOutstandingPromise() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3));

      assertThat(it.hasNext()).isTrue();
      expireDeadline(it, false);
      it.reset();

      assertThat(it.hasNext()).isFalse();
    }

    /**
     * The skip pre-loop is the one path in {@code hasNext()} that bypasses the promise entirely. An expiry reaching it
     * must stop the iteration outright and, crucially, leave nothing promised behind for {@code next()} to deliver.
     */
    @Test
    void expiryReachingTheSkipLoopStopsWithoutPromising() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());
      it.setSkip(2);
      expireDeadline(it, false);

      assertThat(it.hasNext()).isFalse();
      assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    }

    /**
     * Nothing about the ordinary, un-expired iteration changes - skip and limit included, since both are enforced
     * through the very {@code hasNext()} path that now caches its verdict.
     */
    @Test
    void unexpiredIterationIsUnaffectedBySkipAndLimit() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3, 4, 5).iterator());
      it.setTimeout(TimeUnit.MINUTES.toMillis(10), false);
      it.setSkip(1);
      it.setLimit(2);

      final List<Integer> got = new ArrayList<>();
      while (it.hasNext())
        got.add(it.next());

      assertThat(got).containsExactly(2, 3);
    }

    /**
     * Puts the iterator's deadline in the past without sleeping: {@code beginTime} is captured at construction, so a
     * zero-millisecond budget is already spent once the clock has ticked at least once past it.
     */
    private static void expireDeadline(final MultiIterator<?> it, final boolean exceptionOnTimeout) {
      it.setTimeout(0, exceptionOnTimeout);
      // GUARANTEE STRICTLY MORE THAN 0 ms HAVE ELAPSED SINCE beginTime, SINCE THE CHECK IS `elapsed > timeout`
      final long begin = System.currentTimeMillis();
      while (System.currentTimeMillis() == begin)
        Thread.onSpinWait();
    }
  }
}
