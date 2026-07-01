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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4803 (busy-wait portion): {@code ArcadeDbGrpcService.waitUntilReady}
 * used to spin {@code Thread.sleep(1)} with no deadline while the client transport stayed not-ready,
 * pinning the gRPC worker thread (and the open ResultSet/transaction) for a slow or abandoned stream.
 *
 * <p>The fix bounds the wait by a configurable deadline. This test exercises the extracted, transport-free
 * helper {@link ArcadeDbGrpcService#awaitTransportReady} directly so the deadline behaviour is deterministic
 * without needing a real stalled gRPC channel.
 */
class Issue4803WaitUntilReadyTimeoutTest {

  @Test
  void abortsAfterDeadlineWhenNeverReady() {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final long timeoutMs = 150;

    final long start = System.currentTimeMillis();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> false, cancelled, timeoutMs);
    final long elapsed = System.currentTimeMillis() - start;

    // The wait must give up (return not-ready) instead of spinning forever.
    assertThat(ready).as("never-ready transport must time out, not block forever").isFalse();
    // It must have waited at least the configured deadline ...
    assertThat(elapsed).as("must honor the configured deadline").isGreaterThanOrEqualTo(timeoutMs);
    // ... but not run away well past it (generous upper bound to stay CI-stable).
    assertThat(elapsed).as("must not spin indefinitely past the deadline").isLessThan(timeoutMs + 5_000L);
  }

  @Test
  void returnsImmediatelyWhenReady() {
    final AtomicBoolean cancelled = new AtomicBoolean(false);

    final long start = System.currentTimeMillis();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> true, cancelled, 60_000L);
    final long elapsed = System.currentTimeMillis() - start;

    assertThat(ready).as("an already-ready transport must return ready").isTrue();
    assertThat(elapsed).as("must not wait when already ready").isLessThan(1_000L);
  }

  @Test
  void returnsPromptlyWhenAlreadyCancelled() {
    final AtomicBoolean cancelled = new AtomicBoolean(true);

    final long start = System.currentTimeMillis();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> false, cancelled, 60_000L);
    final long elapsed = System.currentTimeMillis() - start;

    assertThat(ready).as("a cancelled stream must not report ready").isFalse();
    assertThat(elapsed).as("a cancelled stream must abort promptly, not wait for the deadline").isLessThan(1_000L);
  }
}
