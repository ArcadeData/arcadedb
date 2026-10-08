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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.QueryAdmissionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9518: the gate that starts the queries received over HTTP in arrival order, when there is a free slot and the
 * running queries leave enough of the heap budget, and makes the others wait instead of refusing them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class QueryAdmissionGateTest {
  private static final long MB = 1024 * 1024;

  // A FRESH GATE PER TEST: THE COUNTERS AND THE QUEUE OF THE JVM-WIDE ONE ARE SHARED WITH EVERY OTHER TEST
  private final QueryAdmissionGate gate    = new QueryAdmissionGate();
  private final List<Thread>       waiters = new ArrayList<>();

  @AfterEach
  void tearDown() throws InterruptedException {
    for (final Thread t : waiters) {
      t.interrupt();
      t.join(10_000);
    }
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_ADMISSION_HEAP_WATERMARK.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.reset();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void enabledByDefaultWithTwiceTheCoresAndAtLeastFourSlots() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    assertThat(GlobalConfiguration.QUERY_MAX_CONCURRENT.getValueAsInteger())
        .isEqualTo(Math.max(4, 2 * Runtime.getRuntime().availableProcessors()));
    assertThat(gate.isEnabled()).isTrue();
  }

  @Test
  void disabledEveryQueryStartsAtOnceAndNothingIsCounted() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(0);
    assertThat(gate.isEnabled()).isFalse();

    final List<QueryAdmissionGate.Ticket> tickets = new ArrayList<>();
    for (int i = 0; i < 1_000; i++)
      tickets.add(gate.admit());

    assertThat(gate.getRunning()).isZero();
    assertThat(gate.getAdmitted()).isZero();
    for (final QueryAdmissionGate.Ticket ticket : tickets)
      ticket.close();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void capsTheRunningQueriesAndStartsTheWaitingOnesInArrivalOrder() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    final QueryAdmissionGate.Ticket first = gate.admit();
    assertThat(gate.getRunning()).isEqualTo(1);

    final int waiting = 8;
    final List<Integer> started = Collections.synchronizedList(new ArrayList<>());
    final List<CompletableFuture<Void>> done = new ArrayList<>();
    for (int i = 0; i < waiting; i++) {
      final int id = i;
      final CompletableFuture<Void> future = new CompletableFuture<>();
      done.add(future);
      startWaiter(() -> {
        try {
          try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
            started.add(id);
            assertThat(gate.getRunning()).isEqualTo(1);
          }
          future.complete(null);
        } catch (final Throwable t) {
          future.completeExceptionally(t);
        }
      });
      // THE NEXT ONE ARRIVES ONLY ONCE THIS ONE IS IN THE QUEUE: THE ARRIVAL ORDER IS THE ORDER OF THE IDS
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == id + 1);
    }

    assertThat(started).as("nothing starts while the only slot is taken").isEmpty();
    first.close();

    for (final CompletableFuture<Void> future : done)
      future.get(30, TimeUnit.SECONDS);
    assertThat(started).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    assertThat(gate.getRunning()).isZero();
    assertThat(gate.getQueued()).isZero();
    assertThat(gate.getAdmitted()).isEqualTo(waiting + 1);
    assertThat(gate.getAdmittedAfterWaiting()).isEqualTo(waiting);
    assertThat(gate.getRefused()).isZero();
  }

  @Test
  void aQueryThatWaitsTooLongIsRefusedWithARetryableErrorAndLeavesTheQueue() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(50L);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(admitElsewhere()).isInstanceOf(QueryAdmissionException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_QUEUE_TIMEOUT.getKey())
          .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.RETRY));
      assertThat(gate.getQueued()).isZero();
      assertThat(gate.getRefused()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();

    // THE SLOT IS FREE AGAIN: THE NEXT QUERY STARTS AT ONCE
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getRunning()).isEqualTo(1);
    }
  }

  @Test
  void aQueryThatFindsTheQueueFullIsRefusedAtOnce() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.setValue(1);
    // LONG ENOUGH THAT A QUERY WAITING INSTEAD OF BEING REFUSED WOULD FAIL WITH THE TIMEOUT MESSAGE, NOT THE QUEUE ONE
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    final QueryAdmissionGate.Ticket first = gate.admit();
    final CompletableFuture<Void> queued = new CompletableFuture<>();
    startWaiter(() -> {
      try {
        gate.admit().close();
        queued.complete(null);
      } catch (final Throwable t) {
        queued.completeExceptionally(t);
      }
    });
    await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == 1);

    assertThat(admitElsewhere()).isInstanceOf(QueryAdmissionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.getKey());
    assertThat(gate.getQueued()).isEqualTo(1);
    assertThat(gate.getRefused()).isEqualTo(1);

    first.close();
    queued.join();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aTimedOutHeadOfTheQueueHandsItsTurnToTheNextOne() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);

    final QueryAdmissionGate.Ticket first = gate.admit();

    // THE HEAD GIVES UP AFTER 3 SECONDS, WHILE THE SECOND ONE IS QUEUED BEHIND IT
    final CompletableFuture<Throwable> head = new CompletableFuture<>();
    startWaiter(() -> {
      try {
        gate.admit(3_000).close();
        head.complete(null);
      } catch (final Throwable t) {
        head.complete(t);
      }
    });
    await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == 1);

    final CompletableFuture<Void> second = new CompletableFuture<>();
    startWaiter(() -> {
      try {
        gate.admit(60_000).close();
        second.complete(null);
      } catch (final Throwable t) {
        second.completeExceptionally(t);
      }
    });
    await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == 2 || head.isDone());

    assertThat(head.get(30, TimeUnit.SECONDS)).isInstanceOf(QueryAdmissionException.class);
    assertThat(gate.getQueued()).isEqualTo(1);

    first.close();
    second.get(30, TimeUnit.SECONDS);
    assertThat(gate.getRunning()).isZero();
    assertThat(gate.getQueued()).isZero();
  }

  @Test
  void aQueryWaitsWhileTheRunningOnesHoldTheHeapBudgetAboveTheWatermark() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(10);
    GlobalConfiguration.QUERY_ADMISSION_HEAP_WATERMARK.setValue(50);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);
    // THE BUDGET IS JVM-WIDE: MEASURED FROM WHAT IT ALREADY HOLDS, SO A RESERVATION LEFT BY ANOTHER TEST CANNOT MATTER
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(QueryHeapBudget.getReservedBytes() / MB + 16);
    final long limit = QueryHeapBudget.getLimitBytes();

    // ONE QUERY RUNNING: THE FIRST QUERY IS ALWAYS ADMITTED WHATEVER THE HEAP, SEE THE NEXT TEST
    final QueryAdmissionGate.Ticket running = gate.admit();

    // THE RUNNING QUERIES HOLD 3/4 OF THE BUDGET
    final long held = limit * 3 / 4 - QueryHeapBudget.getReservedBytes();
    assertThat(QueryHeapBudget.tryReserve(held, limit)).isTrue();
    boolean released = false;
    try {
      final CompletableFuture<Void> admitted = new CompletableFuture<>();
      startWaiter(() -> {
        try {
          gate.admit().close();
          admitted.complete(null);
        } catch (final Throwable t) {
          admitted.completeExceptionally(t);
        }
      });
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == 1);
      // THE SLOT IS FREE, THE HEAP IS NOT: STILL WAITING AFTER SEVERAL POLLS OF THE BUDGET
      await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(30)).until(() -> gate.getHeapDeferrals() > 1);
      assertThat(admitted).isNotDone();
      assertThat(gate.getQueued()).isEqualTo(1);

      // THE RUNNING QUERIES GIVE THE HEAP BACK: THE WAITING ONE STARTS WITHOUT ANY QUERY HAVING ENDED
      QueryHeapBudget.release(held);
      released = true;
      admitted.get(30, TimeUnit.SECONDS);
    } finally {
      if (!released)
        QueryHeapBudget.release(held);
    }
    running.close();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void theFirstQueryStartsWhateverTheHeapSoABudgetHeldByOtherWorkCannotStallTheQueue() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(10);
    GlobalConfiguration.QUERY_ADMISSION_HEAP_WATERMARK.setValue(50);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(QueryHeapBudget.getReservedBytes() / MB + 16);
    final long limit = QueryHeapBudget.getLimitBytes();

    final long held = limit * 3 / 4 - QueryHeapBudget.getReservedBytes();
    assertThat(QueryHeapBudget.tryReserve(held, limit)).isTrue();
    try {
      // NO GATED QUERY RUNNING: ADMITTED AT ONCE, EVEN WITH A TIMEOUT OF 0
      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        assertThat(gate.getRunning()).isEqualTo(1);
        // A SECOND ONE, FROM ANOTHER CLIENT, CANNOT: THE BUDGET IS ABOVE THE WATERMARK AND IT DOES NOT WAIT
        assertThat(admitElsewhere()).isInstanceOf(QueryAdmissionException.class);
      }
    } finally {
      QueryHeapBudget.release(held);
    }
  }

  @Test
  void theWatermarkIsIgnoredWhenTheHeapBudgetIsDisabled() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(10);
    GlobalConfiguration.QUERY_ADMISSION_HEAP_WATERMARK.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);

    // ONE QUERY RUNNING, SO THE FIRST-QUERY RULE DOES NOT APPLY: ADMITTED ONLY BECAUSE THE WATERMARK IS IGNORED
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(admitElsewhere()).isNull();
      assertThat(gate.getAdmitted()).isEqualTo(2);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void anInterruptedWaiterLeavesTheQueueAndKeepsItsInterruptFlag() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final AtomicReference<Boolean> interrupted = new AtomicReference<>();
      final Thread waiter = startWaiter(() -> {
        try {
          gate.admit().close();
        } catch (final Throwable t) {
          error.set(t);
          interrupted.set(Thread.currentThread().isInterrupted());
        }
      });
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == 1);
      waiter.interrupt();
      waiter.join(30_000);

      assertThat(error.get()).isInstanceOf(QueryAdmissionException.class);
      assertThat(interrupted.get()).isTrue();
      assertThat(gate.getQueued()).isZero();
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aQueryStartedFromInsideAnAdmittedOneSharesItsSlotInsteadOfWaitingForIt() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    // NO WAITING: A NESTED QUERY THAT QUEUED FOR THE ONLY SLOT, WHICH ITS OWN CALLER HOLDS, WOULD BE REFUSED AT ONCE
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final QueryAdmissionGate.Ticket outer = gate.admit();
    final QueryAdmissionGate.Ticket inner = gate.admit();
    final QueryAdmissionGate.Ticket innermost = gate.admit();
    assertThat(gate.getRunning()).isEqualTo(1);
    assertThat(gate.getAdmitted()).as("only the outermost query is counted").isEqualTo(1);

    // CLOSED IN ANY ORDER: THE SLOT GOES BACK WITH THE LAST TICKET
    outer.close();
    assertThat(gate.getRunning()).isEqualTo(1);
    innermost.close();
    assertThat(gate.getRunning()).isEqualTo(1);
    inner.close();
    assertThat(gate.getRunning()).isZero();

    // THE THREAD HOLDS NOTHING ANY MORE: ITS NEXT QUERY TAKES A SLOT OF ITS OWN
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getAdmitted()).isEqualTo(2);
    }
  }

  @Test
  void anotherThreadDoesNotShareTheSlot() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final CompletableFuture<Throwable> other = new CompletableFuture<>();
      startWaiter(() -> {
        try {
          gate.admit().close();
          other.complete(null);
        } catch (final Throwable t) {
          other.complete(t);
        }
      });
      assertThat(other.get(30, TimeUnit.SECONDS)).isInstanceOf(QueryAdmissionException.class);
    }
  }

  @Test
  void aTicketClosedOnAnotherThreadGivesTheSlotBackAndTheOwnerQueuesAgain() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final QueryAdmissionGate.Ticket ticket = gate.admit();
    final Thread closer = startWaiter(ticket::close);
    closer.join(30_000);
    assertThat(gate.getRunning()).isZero();

    // THIS THREAD'S SLOT WAS GIVEN BACK ELSEWHERE: ITS NEXT QUERY IS ADMITTED AND COUNTED, NOT WAVED THROUGH
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getRunning()).isEqualTo(1);
      assertThat(gate.getAdmitted()).isEqualTo(2);
    }
  }

  @Test
  void aRequestWaitingOnAnotherServerGivesItsSlotBackOnceAndEarly() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final QueryAdmissionGate.Ticket outer = gate.admit();
    final QueryAdmissionGate.Ticket nested = gate.admit();
    assertThat(admitElsewhere()).as("the only slot is taken").isInstanceOf(QueryAdmissionException.class);

    // A FOLLOWER ABOUT TO FORWARD THE REQUEST TO THE LEADER: THE SLOT GOES BACK WHILE ITS TICKETS ARE STILL OPEN
    gate.releaseCurrentSlot();
    assertThat(gate.getRunning()).isZero();
    assertThat(admitElsewhere()).as("the leader, in the same JVM, gets the slot").isNull();
    gate.releaseCurrentSlot();
    assertThat(gate.getRunning()).isZero();

    // A QUERY THE THREAD STARTS AFTERWARDS TAKES A SLOT OF ITS OWN INSTEAD OF SHARING THE ONE IT GAVE BACK
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getRunning()).isEqualTo(1);
    }

    // THE TICKETS OF THE REQUEST CLOSE NORMALLY AND GIVE NOTHING BACK A SECOND TIME
    nested.close();
    outer.close();
    assertThat(gate.getRunning()).isZero();
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(admitElsewhere()).as("still exactly one slot").isInstanceOf(QueryAdmissionException.class);
    }

    // NOTHING HELD: NOTHING TO GIVE BACK
    gate.releaseCurrentSlot();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void closingATicketTwiceCountsOnce() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(2);

    final QueryAdmissionGate.Ticket outer = gate.admit();
    final QueryAdmissionGate.Ticket nested = gate.admit();
    assertThat(gate.getRunning()).isEqualTo(1);

    // THE SECOND CLOSE OF THE NESTED TICKET MUST NOT TAKE THE SLOT FROM UNDER THE OUTER ONE
    nested.close();
    nested.close();
    assertThat(gate.getRunning()).isEqualTo(1);
    outer.close();
    outer.close();
    assertThat(gate.getRunning()).isZero();
  }

  /**
   * Many threads admitting at once, half of them starting a nested query and some giving their slot back early: the
   * running queries never exceed the limit, and every slot comes back.
   */
  @Test
  void underContentionTheLimitHoldsAndEverySlotComesBack() throws Exception {
    final int limit = 4;
    final int threads = 16;
    final int iterations = 200;
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(limit);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);
    GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.setValue(threads);

    final AtomicInteger maxSeen = new AtomicInteger();
    final List<CompletableFuture<Void>> done = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int id = t;
      final CompletableFuture<Void> future = new CompletableFuture<>();
      done.add(future);
      startWaiter(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
              maxSeen.accumulateAndGet(gate.getRunning(), Math::max);
              if ((id + i) % 2 == 0)
                gate.admit().close();
              if ((id + i) % 7 == 0)
                gate.releaseCurrentSlot();
            }
          }
          future.complete(null);
        } catch (final Throwable e) {
          future.completeExceptionally(e);
        }
      });
    }

    for (final CompletableFuture<Void> future : done)
      future.get(90, TimeUnit.SECONDS);
    assertThat(maxSeen.get()).as("running queries never exceed the limit").isBetween(1, limit);
    assertThat(gate.getRunning()).isZero();
    assertThat(gate.getQueued()).isZero();
    assertThat(gate.getAdmitted()).as("only the outermost queries are counted").isEqualTo((long) threads * iterations);
    assertThat(gate.getRefused()).isZero();
  }

  /** Disabling the gate while queries wait lets them all start: the head re-reads the setting on its own. */
  @Test
  void disablingTheGateWhileQueriesWaitLetsThemStart() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    final QueryAdmissionGate.Ticket held = gate.admit();
    final List<CompletableFuture<Void>> done = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final int queuedBefore = i;
      final CompletableFuture<Void> future = new CompletableFuture<>();
      done.add(future);
      startWaiter(() -> {
        try {
          gate.admit().close();
          future.complete(null);
        } catch (final Throwable e) {
          future.completeExceptionally(e);
        }
      });
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == queuedBefore + 1);
    }

    // NO SLOT IS GIVEN BACK: ONLY THE SETTING CHANGES
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(0);
    for (final CompletableFuture<Void> future : done)
      future.get(30, TimeUnit.SECONDS);
    assertThat(gate.getQueued()).isZero();

    held.close();
    assertThat(gate.getRunning()).as("every slot taken while the gate was on came back").isZero();
  }

  @Test
  void aTicketTakenBeforeTheGateIsDisabledStillGivesItsSlotBack() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    final QueryAdmissionGate.Ticket ticket = gate.admit();
    assertThat(gate.getRunning()).isEqualTo(1);

    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(0);
    ticket.close();
    assertThat(gate.getRunning()).isZero();
  }

  /** Admits and closes on a thread of its own, which holds no slot: a query of another client. Null when it was admitted. */
  private Throwable admitElsewhere() throws Exception {
    final CompletableFuture<Throwable> outcome = new CompletableFuture<>();
    startWaiter(() -> {
      try {
        gate.admit().close();
        outcome.complete(null);
      } catch (final Throwable t) {
        outcome.complete(t);
      }
    });
    return outcome.get(90, TimeUnit.SECONDS);
  }

  private Thread startWaiter(final Runnable body) {
    final Thread thread = new Thread(body, "query-admission-gate-test-" + waiters.size());
    thread.setDaemon(true);
    waiters.add(thread);
    thread.start();
    return thread;
  }
}
