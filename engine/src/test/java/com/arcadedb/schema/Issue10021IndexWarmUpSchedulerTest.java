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
package com.arcadedb.schema;

import com.arcadedb.schema.IndexWarmUpScheduler.Task;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10021: the indexes a schema reload re-created cold are warmed in the background, one after the other, after the
 * schema has been quiet for a while. Pins the scheduling rules with plain tasks - no clock is asserted on: every
 * ordering below is forced with a latch or with a delay too long to elapse.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10021IndexWarmUpSchedulerTest {
  /** Long enough that a request with it can never come due while a test runs. */
  private static final long NEVER = 600_000L;
  /** A bound for waiting on something that must happen, sized as a hang detector and not as a latency claim. */
  private static final long AWAIT_MS = 60_000L;

  private final IndexWarmUpScheduler scheduler = new IndexWarmUpScheduler("issue10021");
  private final List<String>         ran       = new CopyOnWriteArrayList<>();

  @AfterEach
  void tearDown() {
    scheduler.close();
  }

  @Test
  void tasksRunOneAfterTheOtherInRequestOrder() {
    scheduler.request(List.of(task("a"), task("b"), task("c")), 0);

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).containsExactly("a", "b", "c");
  }

  @Test
  void aNewerRequestReplacesOneThatHasNotComeDueYet() {
    scheduler.request(List.of(task("old-1"), task("old-2")), NEVER);
    scheduler.request(List.of(task("new")), 0);

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).as("the superseded batch holds instances nothing reaches any more").containsExactly("new");
  }

  @Test
  void aNewerRequestAbandonsTheRestOfABatchThatIsRunning() throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    scheduler.request(List.of(new Task("first", () -> {
      firstStarted.countDown();
      await(releaseFirst);
      ran.add("first");
    }), task("second")), 0);
    assertThat(firstStarted.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    scheduler.request(List.of(task("newer")), 0);
    releaseFirst.countDown();

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).as("the task in flight finishes; the rest of its batch is dropped").containsExactly("first", "newer");
  }

  @Test
  void aTaskThatFailsDoesNotStopTheOnesAfterIt() {
    scheduler.request(List.of(new Task("broken", () -> {
      throw new IllegalStateException("expected by the test");
    }), task("after")), 0);

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).containsExactly("after");
  }

  @Test
  void theNamesOfTheTasksNotYetRunAreReportedSoTheNextReloadCanKeepTheirIntent() throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    scheduler.request(List.of(new Task("running", () -> {
      firstStarted.countDown();
      await(releaseFirst);
    }), task("queued")), 0);
    assertThat(firstStarted.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    assertThat(scheduler.unfinishedNames()).containsExactlyInAnyOrder("running", "queued");

    releaseFirst.countDown();
    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(scheduler.unfinishedNames()).isEmpty();
  }

  @Test
  void aRequestNotYetDueIsUnfinishedAndIsForgottenWhenReplaced() {
    scheduler.request(List.of(task("waiting")), NEVER);
    assertThat(scheduler.unfinishedNames()).isEqualTo(Set.of("waiting"));

    scheduler.request(List.of(task("replacement")), NEVER);
    assertThat(scheduler.unfinishedNames()).isEqualTo(Set.of("replacement"));
  }

  @Test
  void aClosedSchedulerRunsNothingAndAcceptsNothing() {
    scheduler.request(List.of(task("never")), NEVER);

    scheduler.close();
    scheduler.request(List.of(task("also-never")), 0);

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).isEmpty();
    assertThat(scheduler.unfinishedNames()).isEmpty();
  }

  @Test
  void closeInterruptsALoadInFlightAndWaitsForItToEnd() throws Exception {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch interrupted = new CountDownLatch(1);
    scheduler.request(List.of(new Task("loading", () -> {
      started.countDown();
      try {
        new CountDownLatch(1).await(AWAIT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted by the close", e);
      }
    }), task("after")), 0);
    assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    // The worker has ended when close returns, which is what lets the caller close the files it was reading
    assertThat(scheduler.close(AWAIT_MS)).isTrue();

    assertThat(interrupted.getCount()).as("the load was interrupted").isZero();
    assertThat(ran).as("the rest of the batch never ran").isEmpty();
    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
  }

  @Test
  void closeDoesNotHangOnALoadThatIgnoresTheInterrupt() throws Exception {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    scheduler.request(List.of(new Task("stubborn", () -> {
      started.countDown();
      // A graph build inside JVector does not honour an interrupt: keep going until released
      boolean released = false;
      while (!released)
        try {
          released = release.await(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          // ignored on purpose
        }
    })), 0);
    assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    // Returns, reporting that the worker is still running: the bound, and not the load, decides when close gives up
    assertThat(scheduler.close(1)).isFalse();

    release.countDown();
    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
  }

  /**
   * Issue #10156: the database close stops the warm-up before its flush, and the schema close calls it again as a backstop. The
   * second call must not charge a second bound for a load that already outlived the first.
   */
  @Test
  void aSecondCloseDoesNotWaitAgainForALoadThatOutlivedTheFirst() throws Exception {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    scheduler.request(List.of(new Task("stubborn", () -> {
      started.countDown();
      boolean released = false;
      while (!released)
        try {
          released = release.await(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          // ignored on purpose, as a JVector graph build does
        }
    })), 0);
    assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
    assertThat(scheduler.close(1)).isFalse();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(scheduler.close(AWAIT_MS)).as("still running, and said so").isFalse();
    stopwatch.assertGaveUpWithin(AWAIT_MS / 2, "a backstop close that returns at once from one that joins the worker again");

    release.countDown();
    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(scheduler.close(AWAIT_MS)).as("nothing left running").isTrue();
  }

  @Test
  void anEmptyRequestCancelsWhatWasWaiting() {
    scheduler.request(List.of(task("waiting")), NEVER);
    scheduler.request(List.of(), 0);

    assertThat(scheduler.awaitIdle(AWAIT_MS)).isTrue();
    assertThat(ran).isEmpty();
    assertThat(scheduler.unfinishedNames()).isEmpty();
  }

  private Task task(final String name) {
    return new Task(name, () -> ran.add(name));
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await(AWAIT_MS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
