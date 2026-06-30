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
package com.arcadedb.server.grpc;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4801: gRPC {@code insertBidirectional} / {@code graphBatchLoad} called
 * terminal {@code onNext}/{@code onError}/{@code onCompleted} on a non-thread-safe StreamObserver
 * from multiple threads, which could interleave terminal calls and throw IllegalStateException
 * ("call already closed"), drop responses, or hang the client.
 *
 * <p>The fix funnels every write/terminal call through {@link SynchronizedStreamObserver}, which
 * serializes calls and guarantees at most one delegated terminal call with no {@code onNext} after
 * it. These tests exercise that contract against a delegate that mimics gRPC's real behaviour of
 * throwing once the call is closed.
 */
class Issue4801SynchronizedStreamObserverTest {

  /**
   * Delegate that emulates the gRPC ServerCallStreamObserver contract: any call after a terminal
   * one (onError/onCompleted) throws IllegalStateException, and onNext after terminal also throws.
   * It also records how many terminal calls and post-terminal calls actually reached it.
   */
  private static final class ContractEnforcingObserver implements StreamObserver<String> {
    final AtomicInteger        onNextCount   = new AtomicInteger();
    final AtomicInteger        terminalCount = new AtomicInteger();
    final AtomicInteger        violations    = new AtomicInteger();
    private final AtomicBoolean closed        = new AtomicBoolean(false);

    @Override
    public void onNext(final String value) {
      if (closed.get()) {
        violations.incrementAndGet();
        throw new IllegalStateException("call already closed");
      }
      onNextCount.incrementAndGet();
    }

    @Override
    public void onError(final Throwable t) {
      if (!closed.compareAndSet(false, true)) {
        violations.incrementAndGet();
        throw new IllegalStateException("call already closed");
      }
      terminalCount.incrementAndGet();
    }

    @Override
    public void onCompleted() {
      if (!closed.compareAndSet(false, true)) {
        violations.incrementAndGet();
        throw new IllegalStateException("call already closed");
      }
      terminalCount.incrementAndGet();
    }
  }

  @Test
  void onNextAfterCompletedIsDropped() {
    final ContractEnforcingObserver delegate = new ContractEnforcingObserver();
    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(delegate);

    obs.onNext("a");
    obs.onCompleted();
    // Late writes/terminals must be silently dropped, never delegated.
    obs.onNext("b");
    obs.onCompleted();
    obs.onError(new RuntimeException("late"));

    assertThat(delegate.onNextCount.get()).isEqualTo(1);
    assertThat(delegate.terminalCount.get()).isEqualTo(1);
    assertThat(delegate.violations.get()).isZero();
    assertThat(obs.isTerminated()).isTrue();
  }

  @Test
  void happyPathDeliversExactlyOneTerminalToDelegate() {
    final ContractEnforcingObserver delegate = new ContractEnforcingObserver();
    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(delegate);

    obs.onNext("a");
    obs.onNext("b");
    obs.onCompleted();

    assertThat(delegate.onNextCount.get()).isEqualTo(2);
    assertThat(delegate.terminalCount.get()).isEqualTo(1);
    assertThat(delegate.violations.get()).isZero();
  }

  @Test
  void duplicateTerminalCallsAreCollapsedToOne() {
    final ContractEnforcingObserver delegate = new ContractEnforcingObserver();
    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(delegate);

    obs.onError(new RuntimeException("first"));
    obs.onError(new RuntimeException("second"));
    obs.onCompleted();

    assertThat(delegate.terminalCount.get()).isEqualTo(1);
    assertThat(delegate.violations.get()).isZero();
  }

  @Test
  void markTerminatedDropsSubsequentCallsWithoutDelegating() {
    final ContractEnforcingObserver delegate = new ContractEnforcingObserver();
    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(delegate);

    // Simulates the gRPC cancel handler firing: the transport already closed the call.
    obs.markTerminated();

    obs.onNext("late");
    obs.onCompleted();
    obs.onError(new RuntimeException("late"));

    assertThat(delegate.onNextCount.get()).isZero();
    assertThat(delegate.terminalCount.get()).isZero();
    assertThat(delegate.violations.get()).isZero();
  }

  @Test
  void delegateThrowingOnConcurrentCloseIsSwallowedAndTerminates() {
    // Delegate that throws as if the call was concurrently closed by the transport mid-onNext.
    final AtomicInteger attempts = new AtomicInteger();
    final StreamObserver<String> closing = new StreamObserver<>() {
      @Override
      public void onNext(final String value) {
        attempts.incrementAndGet();
        throw new IllegalStateException("call already closed");
      }

      @Override
      public void onError(final Throwable t) {
      }

      @Override
      public void onCompleted() {
      }
    };

    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(closing);

    // Must not propagate the IllegalStateException to the engine/executor caller.
    obs.onNext("x");
    assertThat(obs.isTerminated()).isTrue();

    // Subsequent calls are no-ops; the delegate is not touched again.
    obs.onNext("y");
    obs.onCompleted();
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  void failedWriteOnLiveCallDeliversTerminalSoClientDoesNotHang() {
    // Delegate whose onNext rejects this one message but is otherwise live (records onError).
    final AtomicInteger onNextAttempts = new AtomicInteger();
    final AtomicReference<Throwable> deliveredError = new AtomicReference<>();
    final StreamObserver<String> liveButRejecting = new StreamObserver<>() {
      @Override
      public void onNext(final String value) {
        onNextAttempts.incrementAndGet();
        throw new IllegalStateException("message rejected");
      }

      @Override
      public void onError(final Throwable t) {
        deliveredError.compareAndSet(null, t);
      }

      @Override
      public void onCompleted() {
      }
    };

    final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(liveButRejecting);

    obs.onNext("x");

    // A terminal must have been delivered so the client is not left hanging.
    assertThat(obs.isTerminated()).isTrue();
    assertThat(deliveredError.get()).isInstanceOf(IllegalStateException.class);

    // The stream is terminal now: further calls are dropped.
    obs.onNext("y");
    obs.onCompleted();
    assertThat(onNextAttempts.get()).isEqualTo(1);
  }

  @Test
  void concurrentTerminalAndWriteCallsNeverInterleave() throws Exception {
    final int threads = 32;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int iter = 0; iter < 200; iter++) {
        final ContractEnforcingObserver delegate = new ContractEnforcingObserver();
        final SynchronizedStreamObserver<String> obs = new SynchronizedStreamObserver<>(delegate);

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicReference<Throwable> escaped = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
          final int idx = t;
          pool.submit(() -> {
            try {
              start.await();
              // Mix of writes, terminals and a simulated cancel, racing on the same observer.
              switch (idx % 4) {
              case 0 -> obs.onNext("v" + idx);
              case 1 -> obs.onCompleted();
              case 2 -> obs.onError(new RuntimeException("boom"));
              default -> obs.markTerminated();
              }
            } catch (final Throwable th) {
              escaped.compareAndSet(null, th);
            } finally {
              done.countDown();
            }
          });
        }

        start.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

        // No exception may escape to the caller, and the delegate must never see a contract
        // violation (a second terminal or a write after close).
        assertThat(escaped.get()).isNull();
        assertThat(delegate.terminalCount.get()).isLessThanOrEqualTo(1);
        assertThat(delegate.violations.get()).isZero();
      }
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }
}
