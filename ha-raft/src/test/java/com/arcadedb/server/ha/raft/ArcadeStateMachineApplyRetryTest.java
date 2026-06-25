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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  void unexpectedErrorOnDivergedStateIsWrappedAsReplicationException() {
    // Regression guard for issue #4740: when a WAL version gap has already diverged the state,
    // subsequent apply operations may throw unexpected errors (NPE, ClassCastException, etc.) from
    // operating on the inconsistent in-memory state. Instead of reaching the fatal server-halt path,
    // they must be wrapped as ReplicationException (recoverable resync) while the snapshot download
    // catches up.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.markStateDiverged();
    final NullPointerException npe = new NullPointerException("inconsistent-state NPE after WAL gap");
    assertThatThrownBy(() -> sm.applyWithRetry(10L, () -> { throw npe; }))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("snapshot resync")
        .hasCause(npe);
  }

  @Test
  void unexpectedErrorOnCleanStateIsRethrown() {
    // Regression guard: an unexpected error on a node whose state is NOT diverged must still reach
    // applyTransaction's fatal catch (Throwable) handler (server-halt path) so real bugs are caught.
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    // stateDivergedSinceWalGap is false by default - do NOT call markStateDiverged().
    final NullPointerException npe = new NullPointerException("programming bug on healthy node");
    assertThatThrownBy(() -> sm.applyWithRetry(11L, () -> { throw npe; })).isSameAs(npe);
  }
}
