/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9561: {@code RaftHAServer.stop()} and the in-place Ratis restart closed the Ratis server with no bound, so a
 * close that never returned hung the shutdown (and the JVM shutdown hook behind it), or held the recovery lock for good.
 * The close now runs on its own thread, bounded by {@code arcadedb.ha.ratisCloseTimeoutMs}.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only: the unbounded close this guards never returns
class Issue9561BoundedRatisServerCloseTest {

  private static final String SERVER_LIST      = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   CLOSE_TIMEOUT_MS = 300L;
  /** Far above CLOSE_TIMEOUT_MS and far below "never": separates a bounded wait from the unbounded one. */
  private static final long   GAVE_UP_BOUND_MS = 20_000L;

  private final CountDownLatch release = new CountDownLatch(1);

  @AfterEach
  void releaseStuckCloses() {
    release.countDown();
  }

  // ---- RatisServerCloser ----

  @Test
  void aCloseThatFinishesInTimeReturnsNoThread() throws IOException {
    final AtomicInteger closes = new AtomicInteger();
    assertThat(RatisServerCloser.close("test", closes::incrementAndGet, 10_000L)).isNull();
    assertThat(closes.get()).isEqualTo(1);
  }

  @Test
  void aCloseThatHangsIsLeftRunningAfterTheBound() throws Exception {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Thread stuck = RatisServerCloser.close("test", this::blockUntilReleased, CLOSE_TIMEOUT_MS);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bounded Ratis close from one that never returns");

    assertThat(stuck).isNotNull();
    assertThat(stuck.isAlive()).isTrue();
    assertThat(stuck.isDaemon()).as("a stuck close must not keep the JVM alive").isTrue();

    release.countDown();
    stuck.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(stuck.isAlive()).as("the close finishes once it is unblocked").isFalse();
  }

  @Test
  void aZeroTimeoutWaitsForTheCloseWithoutABound() throws IOException {
    final AtomicInteger closes = new AtomicInteger();
    // far longer than the 1ms a "0 means at once" reading would allow
    assertThat(RatisServerCloser.close("test", () -> {
      try {
        Thread.sleep(300L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      closes.incrementAndGet();
    }, 0L)).isNull();
    assertThat(closes.get()).isEqualTo(1);
  }

  @Test
  void theFailureOfACloseThatFinishedInTimeReachesTheCaller() {
    assertThatThrownBy(() -> RatisServerCloser.close("test", () -> {
      throw new IOException("boom");
    }, 10_000L)).isInstanceOf(IOException.class).hasMessage("boom");
    assertThatThrownBy(() -> RatisServerCloser.close("test", () -> {
      throw new IllegalStateException("bang");
    }, 10_000L)).isInstanceOf(IllegalStateException.class).hasMessage("bang");
  }

  @Test
  void anInterruptedCallerStopsWaitingAndKeepsItsInterruptFlag() throws Exception {
    final Thread[] result = new Thread[1];
    final boolean[] interruptedAfter = new boolean[1];
    final Thread caller = new Thread(() -> {
      Thread.currentThread().interrupt();
      try {
        result[0] = RatisServerCloser.close("test", this::blockUntilReleased, TimeUnit.MINUTES.toMillis(10));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      interruptedAfter[0] = Thread.currentThread().isInterrupted();
    }, "issue9561-interrupted-caller");
    caller.start();
    caller.join(TimeUnit.SECONDS.toMillis(30));

    assertThat(caller.isAlive()).as("an interrupt ends the wait").isFalse();
    assertThat(result[0]).isNotNull();
    assertThat(interruptedAfter[0]).isTrue();
  }

  // ---- RaftHAServer ----

  @Test
  void stopReturnsWhenTheRatisServerCloseHangs() throws Exception {
    final RaftHAServer raft = detachedServer();
    final HangingServer old = new HangingServer();
    setField(raft, "raftServer", old.proxy());

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    raft.stop();
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bounded Ratis close on stop() from one that never returns");

    assertThat(old.closes.get()).isEqualTo(1);
    assertThat(getField(raft, "raftServer")).isNull();
  }

  @Test
  void stopAfterARestartCloseTimedOutWaitsForThatCloseInsteadOfClosingAgain() throws Exception {
    final RaftHAServer raft = detachedServer();
    final HangingServer old = new HangingServer();
    setField(raft, "raftServer", old.proxy());
    raft.restartRatisIfNeeded();
    assertThat(old.closes.get()).isEqualTo(1);

    // A second close() on a CLOSING Ratis server is a no-op that still interrupts the close in flight (#8900).
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    raft.stop();
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bounded wait on stop() for a close that never returns");

    assertThat(old.closes.get()).as("stop() must not close a server another thread is closing").isEqualTo(1);
    assertThat(getField(raft, "raftServer")).isNull();
  }

  @Test
  void aRestartWhoseOldServerCloseHangsFailsAndKeepsFailingWhileItRuns() throws Exception {
    final RaftHAServer raft = detachedServer();
    final HangingServer old = new HangingServer();
    final RaftServer oldProxy = old.proxy();
    setField(raft, "raftServer", oldProxy);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    raft.restartRatisIfNeeded();
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bounded Ratis close on restart from one that never returns");

    assertThat(old.closes.get()).isEqualTo(1);
    assertThat(getField(raft, "raftServer")).as("no new server beside the one still closing").isSameAs(oldProxy);
    assertThat(getField(raft, "restartFailureCount")).as("counts toward the escalation").isEqualTo(1);
    assertThat(raft.getRecoverRestartCount()).isZero();

    // The next health tick: the close is still running, so the attempt fails again without a second close.
    raft.restartRatisIfNeeded();
    assertThat(old.closes.get()).isEqualTo(1);
    assertThat(getField(raft, "raftServer")).isSameAs(oldProxy);
    assertThat(getField(raft, "restartFailureCount")).isEqualTo(2);

    // Once the close finishes, the gate opens again: the next attempt gets past it and closes the old server again (a
    // no-op on a real Ratis server). The fake's second close requests a shutdown, so the attempt ends there instead of
    // starting a real Ratis server.
    final Thread stuck = (Thread) getField(raft, "stuckRatisClose");
    assertThat(stuck).isNotNull();
    old.onLaterClose = () -> setFieldUnchecked(raft, "shutdownRequested", true);
    release.countDown();
    stuck.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(stuck.isAlive()).isFalse();

    raft.restartRatisIfNeeded();
    assertThat(getField(raft, "stuckRatisClose")).as("the gate is released").isNull();
    assertThat(old.closes.get()).as("the attempt got past the gate").isEqualTo(2);
    assertThat(getField(raft, "restartFailureCount")).as("an abandoned attempt is not a failure").isEqualTo(2);
  }

  @Test
  void theRestartFailuresOfAStuckCloseSpendTheEscalationBudget() throws Exception {
    final RaftHAServer raft = detachedServer();
    final HangingServer old = new HangingServer();
    setField(raft, "raftServer", old.proxy());
    final int maxRetries = GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES.getValueAsInteger();

    for (int i = 0; i < maxRetries; i++)
      raft.restartRatisIfNeeded();
    assertThat(getField(raft, "restartFailureCount")).isEqualTo(maxRetries);
    assertThat(old.closes.get()).as("one close, however many attempts").isEqualTo(1);
  }

  private void blockUntilReleased() {
    try {
      release.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** A Ratis server whose close never returns until the test releases it. */
  private final class HangingServer {
    final AtomicInteger closes = new AtomicInteger();
    volatile LifeCycle.State state = LifeCycle.State.RUNNING;
    volatile Runnable        onLaterClose;

    RaftServer proxy() {
      return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[] { RaftServer.class },
          (self, method, args) -> switch (method.getName()) {
            case "close" -> {
              if (closes.incrementAndGet() > 1 && onLaterClose != null)
                onLaterClose.run();
              if (state != LifeCycle.State.RUNNING)
                yield null; // like Ratis: a close runs at most once
              state = LifeCycle.State.CLOSING;
              blockUntilReleased();
              state = LifeCycle.State.CLOSED;
              yield null;
            }
            case "getLifeCycleState" -> state;
            case "getId" -> RaftPeerId.valueOf("issue9561");
            case "getGroupIds" -> List.of();
            case "getDivision" -> throw new IOException("no division");
            case "toString" -> "HangingServer";
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            default -> null;
          });
    }
  }

  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    config.setValue(GlobalConfiguration.HA_RATIS_CLOSE_TIMEOUT_MS, CLOSE_TIMEOUT_MS);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    return new RaftHAServer(arcadeServer, config);
  }

  private static void setField(final Object target, final String name, final Object value) throws Exception {
    final Field field = RaftHAServer.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void setFieldUnchecked(final Object target, final String name, final Object value) {
    try {
      setField(target, name, value);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static Object getField(final Object target, final String name) throws Exception {
    final Field field = RaftHAServer.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
