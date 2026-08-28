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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
public class Issue6880MultiIteratorTimeoutBetweenHasNextAndNextTest {

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
