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
package com.arcadedb.graph.olap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10249: the changes of the committed transactions reach a Graph Analytical View in the order the transactions committed,
 * whichever thread gets to the view first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CommitOrderGateTest {
  private final CommitOrderGate gate      = new CommitOrderGate(() -> 30_000L);
  private final List<TxDelta>   delivered = new CopyOnWriteArrayList<>();

  @Test
  void aChangeIsDeliveredAtOnceWhenItIsFirstInLine() {
    final TxDelta delta = new TxDelta();
    final CommitOrderGate.Place place = gate.takePlace();

    gate.deliver(place, delta, delivered::add);

    assertThat(delivered).containsExactly(delta);
    assertThat(gate.pending()).isZero();
  }

  @Test
  void aChangeWaitsForTheOnesAheadOfItAndTheCallerStaysUntilItIsDelivered() throws Exception {
    final TxDelta first = new TxDelta();
    final TxDelta second = new TxDelta();
    final TxDelta third = new TxDelta();
    final CommitOrderGate.Place firstPlace = gate.takePlace();
    final CommitOrderGate.Place secondPlace = gate.takePlace();
    final CommitOrderGate.Place thirdPlace = gate.takePlace();

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // The later transactions get to the view first
      final Future<?> thirdDelivery = executor.submit(() -> gate.deliver(thirdPlace, third, delivered::add));
      final Future<?> secondDelivery = executor.submit(() -> gate.deliver(secondPlace, second, delivered::add));

      Thread.sleep(200);
      assertThat(delivered).as("nothing is delivered ahead of the first place").isEmpty();
      assertThat(thirdDelivery.isDone()).as("the caller waits for its change to be delivered").isFalse();
      assertThat(secondDelivery.isDone()).isFalse();

      gate.deliver(firstPlace, first, delivered::add);

      secondDelivery.get(1, TimeUnit.MINUTES);
      thirdDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactly(first, second, third);
    assertThat(gate.pending()).isZero();
  }

  @Test
  void anAbandonedPlaceReleasesTheOnesBehindIt() throws Exception {
    final TxDelta second = new TxDelta();
    final CommitOrderGate.Place firstPlace = gate.takePlace();
    final CommitOrderGate.Place secondPlace = gate.takePlace();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> secondDelivery = executor.submit(() -> gate.deliver(secondPlace, second, delivered::add));
      Thread.sleep(100);
      assertThat(secondDelivery.isDone()).isFalse();

      gate.abandon(firstPlace, delivered::add);

      secondDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactly(second);
  }

  @Test
  void aTransactionWithNothingToDeliverOnlyReleasesItsPlace() throws Exception {
    final TxDelta second = new TxDelta();
    final CommitOrderGate.Place firstPlace = gate.takePlace();
    final CommitOrderGate.Place secondPlace = gate.takePlace();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> secondDelivery = executor.submit(() -> gate.deliver(secondPlace, second, delivered::add));
      Thread.sleep(100);

      gate.deliver(firstPlace, null, delivered::add);

      secondDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactly(second);
  }

  @Test
  @Timeout(20)
  void aCommitMadeByACallbackOfAnotherCommitOfTheSameThreadDoesNotWaitForIt() {
    // The outer commit took its place first and is still running its callbacks when the inner one commits on the same thread
    final TxDelta outer = new TxDelta();
    final TxDelta inner = new TxDelta();
    final CommitOrderGate.Place outerPlace = gate.takePlace();
    final CommitOrderGate.Place innerPlace = gate.takePlace();

    gate.deliver(innerPlace, inner, delivered::add);
    assertThat(delivered).as("the inner change waits for the outer one").isEmpty();

    gate.deliver(outerPlace, outer, delivered::add);
    assertThat(delivered).containsExactly(outer, inner);
  }

  @Test
  @Timeout(20)
  void aPlaceThatNeverReportsIsEvictedAndItsLateChangeStillArrives() throws Exception {
    final CommitOrderGate shortWait = new CommitOrderGate(() -> 200L);
    final TxDelta lost = new TxDelta();
    final TxDelta second = new TxDelta();

    // Taken by a thread of its own, like the transaction of a thread that dies before it reports
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CommitOrderGate.Place stuck;
    try {
      stuck = executor.submit(shortWait::takePlace).get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }
    final CommitOrderGate.Place secondPlace = shortWait.takePlace();

    // The wait is over: the stuck place is evicted, and the change behind it goes in
    shortWait.deliver(secondPlace, second, delivered::add);
    assertThat(delivered).containsExactly(second);
    assertThat(shortWait.pending()).isZero();

    // The transaction reports at last: its change is not lost
    shortWait.deliver(stuck, lost, delivered::add);
    assertThat(delivered).containsExactly(second, lost);
  }

  @Test
  @Timeout(20)
  void anErrorFromTheViewDoesNotLeaveTheReadyPlacesBehindItUndelivered() throws Exception {
    final TxDelta first = new TxDelta();
    final TxDelta second = new TxDelta();
    final CommitOrderGate.Place firstPlace = gate.takePlace();
    final CommitOrderGate.Place secondPlace = gate.takePlace();
    final Consumer<TxDelta> view = delta -> {
      if (delta == first)
        throw new StackOverflowError("an error, not an exception");
      delivered.add(delta);
    };

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // Ready and waiting behind the first place, on a thread of its own
      final Future<?> secondDelivery = executor.submit(() -> gate.deliver(secondPlace, second, view));
      Thread.sleep(100);

      // The delivery of the first one ends in an Error before it reaches the second one: the waiting thread takes the line over
      assertThatThrownBy(() -> gate.deliver(firstPlace, first, view)).isInstanceOf(StackOverflowError.class);

      secondDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactly(second);
    assertThat(gate.pending()).isZero();
  }

  @Test
  @Timeout(20)
  void aLateChangeDeliveredWhileAnotherDeliveryIsInProgressIsNotLost() throws Exception {
    final CommitOrderGate shortWait = new CommitOrderGate(() -> 100L);
    final TxDelta lost = new TxDelta();
    final TxDelta second = new TxDelta();
    final CountDownLatch inSink = new CountDownLatch(1);
    final CountDownLatch releaseSink = new CountDownLatch(1);
    final Consumer<TxDelta> view = delta -> {
      if (delta == second) {
        inSink.countDown();
        try {
          releaseSink.await(1, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      delivered.add(delta);
    };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // The place of a transaction that is late, taken by a thread of its own
      final CommitOrderGate.Place stuck = executor.submit(shortWait::takePlace).get(1, TimeUnit.MINUTES);
      final CommitOrderGate.Place secondPlace = shortWait.takePlace();
      // Evicts the stuck place once the wait is over, and delivers the second change: held in the view by the latch
      final Future<?> secondDelivery = executor.submit(() -> shortWait.deliver(secondPlace, second, view));
      assertThat(inSink.await(1, TimeUnit.MINUTES)).as("the second change is being delivered").isTrue();

      // The late transaction reports meanwhile: its change goes in beside the delivery in progress
      shortWait.deliver(stuck, lost, view);
      assertThat(delivered).containsExactly(lost);

      releaseSink.countDown();
      secondDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      releaseSink.countDown();
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactlyInAnyOrder(lost, second);
    assertThat(shortWait.pending()).isZero();
  }

  @Test
  void withNoWaitACommitDoesNotWaitForTheOnesAheadButTheOrderIsKept() throws Exception {
    final CommitOrderGate noWait = new CommitOrderGate(() -> 0L);
    final TxDelta first = new TxDelta();
    final TxDelta second = new TxDelta();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CommitOrderGate.Place firstPlace;
    try {
      firstPlace = executor.submit(noWait::takePlace).get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }
    final CommitOrderGate.Place secondPlace = noWait.takePlace();

    // Returns at once, with its change still behind the first one
    noWait.deliver(secondPlace, second, delivered::add);
    assertThat(delivered).isEmpty();

    noWait.deliver(firstPlace, first, delivered::add);
    assertThat(delivered).containsExactly(first, second);
  }

  @Test
  void aPlaceThatIsAbandonedAfterItWasEvictedChangesNothing() throws Exception {
    final CommitOrderGate shortWait = new CommitOrderGate(() -> 100L);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CommitOrderGate.Place stuck;
    try {
      stuck = executor.submit(shortWait::takePlace).get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }
    final TxDelta second = new TxDelta();
    shortWait.deliver(shortWait.takePlace(), second, delivered::add);

    shortWait.abandon(stuck, delivered::add);

    assertThat(delivered).containsExactly(second);
    assertThat(shortWait.pending()).isZero();
  }

  @Test
  void aChangeTheViewCannotTakeDoesNotHoldBackTheOnesBehindIt() throws Exception {
    final TxDelta first = new TxDelta();
    final TxDelta second = new TxDelta();
    final CommitOrderGate.Place firstPlace = gate.takePlace();
    final CommitOrderGate.Place secondPlace = gate.takePlace();
    final Consumer<TxDelta> view = delta -> {
      if (delta == first)
        throw new IllegalStateException("the view cannot take it");
      delivered.add(delta);
    };

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> secondDelivery = executor.submit(() -> gate.deliver(secondPlace, second, view));
      Thread.sleep(100);

      gate.deliver(firstPlace, first, view);

      secondDelivery.get(1, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }

    assertThat(delivered).containsExactly(second);
    assertThat(gate.pending()).isZero();
  }
}
