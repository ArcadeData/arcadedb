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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression test for the Raft apply path's exception handling: a retryable {@code NeedRetryException}
 * (e.g. an MVCC {@code ConcurrentModificationException} from a page-version race) must be retried in
 * place and, if it persists, escalated to a snapshot resync ({@code ReplicationException}) - it must
 * NEVER reach the fatal {@code catch (Throwable)} branch in {@code applyTransaction} that stops the
 * node. A genuine (non-retryable) error must still propagate unchanged so that fatal path still fires.
 */
class ArcadeStateMachineApplyRetryTest {
  private int prevRetries;
  private int prevDelay;

  @BeforeEach
  void setUp() {
    prevRetries = GlobalConfiguration.TX_RETRIES.getValueAsInteger();
    prevDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRIES.setValue(2);     // -> 3 attempts total (maxRetries + 1)
    GlobalConfiguration.TX_RETRY_DELAY.setValue(0);  // no backoff sleep: deterministic and fast
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.TX_RETRIES.setValue(prevRetries);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(prevDelay);
  }

  @Test
  void succeedsOnFirstAttempt() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    assertThatNoException().isThrownBy(() -> sm.applyWithRetry(1L, calls::incrementAndGet));
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void retriesTransientErrorThenSucceeds() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    // Retryable on the first two attempts, succeeds on the third.
    assertThatNoException().isThrownBy(() -> sm.applyWithRetry(2L, () -> {
      if (calls.incrementAndGet() < 3)
        throw new ConcurrentModificationException("page version race - please retry");
    }));
    assertThat(calls.get()).isEqualTo(3);
  }

  @Test
  void escalatesToResyncWhenRetryablePersists() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    final ConcurrentModificationException cme = new ConcurrentModificationException("persistent page race");
    // Always retryable: after exhausting attempts it must escalate to ReplicationException (resync),
    // not crash, and the original retryable cause must be preserved for diagnostics.
    assertThatThrownBy(() -> sm.applyWithRetry(3L, () -> {
      calls.incrementAndGet();
      throw cme;
    }))
        .isInstanceOf(ReplicationException.class)
        .hasCause(cme);
    // attempts == maxRetries + 1, computed from config so it can't rot if setUp() changes.
    assertThat(calls.get()).isEqualTo(GlobalConfiguration.TX_RETRIES.getValueAsInteger() + 1);
  }

  @Test
  void interruptDuringBackoffPreservesFlagAndEscalates() {
    GlobalConfiguration.TX_RETRY_DELAY.setValue(50); // positive delay so the backoff sleep is reached
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    final ConcurrentModificationException cme = new ConcurrentModificationException("page race");
    // Pre-set the interrupt flag so Thread.sleep() throws InterruptedException on the first backoff.
    Thread.currentThread().interrupt();
    try {
      // The retry loop must break out, preserve the interrupt flag, and escalate to a ReplicationException
      // rather than letting the InterruptedException propagate unchecked.
      assertThatThrownBy(() -> sm.applyWithRetry(5L, () -> {
        calls.incrementAndGet();
        throw cme;
      }))
          .isInstanceOf(ReplicationException.class)
          .hasCause(cme);
      assertThat(calls.get()).isEqualTo(1); // interrupted during the first backoff: no further attempts
      // Thread.interrupted() both asserts the flag survived and clears it so it can't leak to other tests.
      assertThat(Thread.interrupted()).isTrue();
    } finally {
      Thread.interrupted(); // defensive: ensure the flag is cleared even if an assertion failed
    }
  }

  @Test
  void zeroRetriesMakesSingleAttemptThenEscalates() {
    GlobalConfiguration.TX_RETRIES.setValue(0); // boundary: exactly one attempt, no retry
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    final ConcurrentModificationException cme = new ConcurrentModificationException("page race");
    // With maxRetries == 0 a retryable error must escalate immediately after the first attempt.
    assertThatThrownBy(() -> sm.applyWithRetry(6L, () -> {
      calls.incrementAndGet();
      throw cme;
    }))
        .isInstanceOf(ReplicationException.class)
        .hasCause(cme);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void doesNotRetryNonRetryableError() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final AtomicInteger calls = new AtomicInteger();
    final IllegalStateException boom = new IllegalStateException("genuine bug - must not be retried");
    // A non-NeedRetryException must propagate immediately and unchanged, so applyTransaction's fatal
    // catch (Throwable) handler still fires for real bugs (state divergence protection).
    assertThatThrownBy(() -> sm.applyWithRetry(4L, () -> {
      calls.incrementAndGet();
      throw boom;
    })).isSameAs(boom);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void unexpectedErrorOnDivergedDatabaseIsWrappedAsReplicationException() {
    // Regression guard for issue #4740: when a WAL version gap has already diverged a database,
    // subsequent apply operations may throw unexpected errors (NPE, ClassCastException, etc.) from
    // operating on the inconsistent in-memory state. Instead of reaching the fatal server-halt path,
    // they must be wrapped as ReplicationException (recoverable resync) while the snapshot download
    // catches up.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged("diverged-db");
    final NullPointerException npe = new NullPointerException("inconsistent-state NPE after WAL gap");
    assertThatThrownBy(() -> sm.applyWithRetry(10L, "diverged-db", () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("snapshot resync")
        .hasCause(npe);
  }

  @Test
  void unexpectedErrorOnHealthyDatabaseIsQuarantinedNotNodeHalted() {
    // Regression guard for issue #4797: a single ArcadeStateMachine multiplexes every database, so an
    // unexpected error applying an entry for one healthy database must NOT trip the node-wide critical
    // halt (which would freeze replication for all co-located databases). Instead the database is
    // quarantined (marked diverged + targeted resync) and the error reported as a recoverable
    // ReplicationException so the node stays up. server == null here, so the targeted resync is a
    // no-op, but the quarantine bookkeeping and the recoverable wrapping still apply.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final NullPointerException npe = new NullPointerException("programming bug on healthy database");
    assertThatThrownBy(() -> sm.applyWithRetry(11L, "healthy-db", () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("snapshot resync")
        .hasCause(npe);
    assertThat(sm.isDatabaseDiverged("healthy-db")).isTrue();
    // The node-wide halt flag must remain untouched: this is the crux of #4797.
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    // A different, untouched database is not quarantined: the failure is scoped to "healthy-db".
    assertThat(sm.isDatabaseDiverged("other-db")).isFalse();
  }

  @Test
  void unexpectedErrorOnGlobalEntryWithoutTargetDatabaseIsRethrown() {
    // Regression guard for issue #4797: an entry that has no single target database (databaseName
    // null or empty, e.g. a SECURITY_USERS_ENTRY) is not isolable to one database's state, so an
    // unexpected error must still propagate unchanged to applyTransaction's fatal catch (Throwable)
    // node-halt path. Verify both the null and the empty-string cases.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final IllegalStateException nullDbBoom = new IllegalStateException("bug applying a global entry");
    assertThatThrownBy(() -> sm.applyWithRetry(11L, null, () -> { throw nullDbBoom; })).isSameAs(nullDbBoom);
    final IllegalStateException emptyDbBoom = new IllegalStateException("bug applying a security entry");
    assertThatThrownBy(() -> sm.applyWithRetry(12L, "", () -> { throw emptyDbBoom; })).isSameAs(emptyDbBoom);
    assertThat(sm.isDatabaseDiverged("")).isFalse();
  }

  @Test
  void errorOnDivergedDatabaseIsRethrownNotWrapped() {
    // Regression guard: a JVM Error (OutOfMemoryError, StackOverflowError, ...) indicates an
    // unstable JVM and must NEVER be swallowed as a recoverable resync condition - even when the
    // database is diverged it must propagate unchanged to the fatal halt path.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged("diverged-db");
    final OutOfMemoryError oom = new OutOfMemoryError("OOM on diverged database");
    assertThatThrownBy(() -> sm.applyWithRetry(12L, "diverged-db", () -> { throw oom; })).isSameAs(oom);
  }

  @Test
  void replicationExceptionOnDivergedDatabaseIsNotDoubleWrapped() {
    // Regression guard: the WAL-gap escalation in applyTxEntry throws a ReplicationException while
    // the database is already in the diverged set. applyWithRetry must propagate it unchanged rather
    // than re-wrapping it (which would lose the original message and consume the escalation budget).
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged("diverged-db");
    final ReplicationException re = new ReplicationException("WAL version gap detected - snapshot resync required");
    assertThatThrownBy(() -> sm.applyWithRetry(13L, "diverged-db", () -> { throw re; })).isSameAs(re);
  }

  @Test
  void boundedEscalationRePropagatesAfterThresholdExceeded() {
    // Regression guard for issue #4740 bounded escalation: a database that can never resync must not
    // swallow unexpected errors forever. The first MAX_DIVERGED_SWALLOWED_ERRORS (100) errors wrap as
    // recoverable ReplicationExceptions; the next one is re-propagated to the fatal halt path so a
    // stuck node surfaces loudly. The threshold check is incrementAndGet() > 100, so calls 1..100 wrap
    // and call 101 re-propagates.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged("stuck-db");
    final NullPointerException npe = new NullPointerException("inconsistent state after WAL gap");
    for (int i = 1; i <= 100; i++)
      assertThatThrownBy(() -> sm.applyWithRetry(100L, "stuck-db", () -> { throw npe; }))
          .as("swallow #%d must wrap as recoverable", i)
          .isInstanceOf(ReplicationException.class);
    assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(100);
    // The 101st swallowed error exceeds the budget and must propagate unchanged.
    assertThatThrownBy(() -> sm.applyWithRetry(101L, "stuck-db", () -> { throw npe; })).isSameAs(npe);
  }

  @Test
  void clearDivergedStateResetsSetAndCounter() {
    // Regression guard: a completed snapshot resync calls clearDivergedState(), which must clear both
    // the diverged-database set and the bounded-escalation counter so the node returns to normal
    // fail-fast behaviour.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged("db");
    final NullPointerException npe = new NullPointerException("inconsistent state after WAL gap");
    for (int i = 0; i < 3; i++)
      assertThatThrownBy(() -> sm.applyWithRetry(200L, "db", () -> { throw npe; }))
          .isInstanceOf(ReplicationException.class);
    assertThat(sm.isDatabaseDiverged("db")).isTrue();
    assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(3);

    sm.clearDivergedState();

    assertThat(sm.isDatabaseDiverged("db")).isFalse();
    assertThat(sm.divergedSwallowedErrorCount()).isZero();
    // After the reset the database is healthy again, so the next unexpected error re-quarantines it
    // (issue #4797: per-database resync, recoverable) rather than reaching the node-wide halt.
    assertThatThrownBy(() -> sm.applyWithRetry(201L, "db", () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class);
    assertThat(sm.isDatabaseDiverged("db")).isTrue();
  }

  @Test
  void divergedResyncLogIsThrottledPerDatabase() {
    // Log-flood guard: once a WAL version gap quarantines a database, every subsequent committed entry
    // for it hits the same gap until the snapshot resync lands. The per-entry "resync in progress"
    // notice must be throttled to at most once per window per database, so a busy database does not
    // emit tens of log lines per second (which both spams the log and starves the download).
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    // First observation for a database logs; an immediate repeat within the window is suppressed.
    assertThat(sm.shouldLogDivergedResync("db-a")).isTrue();
    assertThat(sm.shouldLogDivergedResync("db-a")).isFalse();
    assertThat(sm.shouldLogDivergedResync("db-a")).isFalse();
    // The throttle is per-database: a different database logs independently on its first observation.
    assertThat(sm.shouldLogDivergedResync("db-b")).isTrue();
    assertThat(sm.shouldLogDivergedResync("db-b")).isFalse();
  }

  @Test
  void clearDivergedStateResetsResyncLogThrottle() {
    // A completed full resync clears the diverged set; the throttle state must reset too so that if the
    // database diverges again later the first notice is emitted rather than being wrongly suppressed by
    // a stale timestamp.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.shouldLogDivergedResync("db")).isTrue();
    assertThat(sm.shouldLogDivergedResync("db")).isFalse();
    sm.clearDivergedState();
    assertThat(sm.shouldLogDivergedResync("db")).isTrue();
  }

  @Test
  void clearDivergedDatabaseResetsResyncLogThrottleForThatDatabaseOnly() {
    // A targeted single-database resync must reset only that database's throttle; other quarantined
    // databases keep their throttle state.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.shouldLogDivergedResync("db-a")).isTrue();
    assertThat(sm.shouldLogDivergedResync("db-b")).isTrue();
    assertThat(sm.shouldLogDivergedResync("db-a")).isFalse();
    assertThat(sm.shouldLogDivergedResync("db-b")).isFalse();

    sm.clearDivergedDatabase("db-a");
    assertThat(sm.shouldLogDivergedResync("db-a")).isTrue();  // reset -> logs again
    assertThat(sm.shouldLogDivergedResync("db-b")).isFalse(); // untouched -> still throttled
  }

  @Test
  void clearDivergedDatabaseRemovesOnlyThatDatabaseAndResetsCounterWhenEmpty() {
    // Regression guard for issue #4797: a targeted single-database resync clears only the resynced
    // database, leaving other quarantined databases intact. The shared escalation counter is reset
    // only once no database remains quarantined.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final NullPointerException npe = new NullPointerException("inconsistent state");
    assertThatThrownBy(() -> sm.applyWithRetry(300L, "db-a", () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class);
    assertThatThrownBy(() -> sm.applyWithRetry(301L, "db-b", () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class);
    assertThat(sm.isDatabaseDiverged("db-a")).isTrue();
    assertThat(sm.isDatabaseDiverged("db-b")).isTrue();
    assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(2);

    // Resyncing db-a clears only db-a; db-b stays quarantined and the counter is NOT reset yet.
    sm.clearDivergedDatabase("db-a");
    assertThat(sm.isDatabaseDiverged("db-a")).isFalse();
    assertThat(sm.isDatabaseDiverged("db-b")).isTrue();
    assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(2);

    // Resyncing the last quarantined database resets the shared escalation counter.
    sm.clearDivergedDatabase("db-b");
    assertThat(sm.isDatabaseDiverged("db-b")).isFalse();
    assertThat(sm.divergedSwallowedErrorCount()).isZero();
  }
}
