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

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

// Regression: #4803
/**
 * Regression test for issue #4803 (busy-wait portion): {@code ArcadeDbGrpcService.waitUntilReady}
 * used to spin {@code Thread.sleep(1)} with no deadline while the client transport stayed not-ready,
 * pinning the gRPC worker thread (and the open ResultSet/transaction) for a slow or abandoned stream.
 *
 * <p>The fix bounds the wait by a configurable deadline. This test exercises the extracted, transport-free
 * helper {@link ArcadeDbGrpcService#awaitTransportReady} directly so the deadline behaviour is deterministic
 * without needing a real stalled gRPC channel.
 */
class GrpcWaitUntilReadyTimeoutTest {

  @Test
  void abortsAfterDeadlineWhenNeverReady() {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final long timeoutMs = 150;

    final StallAwareStopwatch clock = StallAwareStopwatch.start();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> false, cancelled, timeoutMs);

    // The wait must give up (return not-ready) instead of spinning forever.
    assertThat(ready).as("never-ready transport must time out, not block forever").isFalse();
    // It must have waited at least the configured deadline ...
    assertThat(clock.elapsedMs()).as("must honor the configured deadline").isGreaterThanOrEqualTo(timeoutMs);
    // ... but not run away well past it (generous upper bound to stay CI-stable).
    clock.assertGaveUpWithin(timeoutMs + 5_000L, "honouring the deadline from spinning past it");
  }

  @Test
  void returnsImmediatelyWhenReady() {
    final AtomicBoolean cancelled = new AtomicBoolean(false);

    final StallAwareStopwatch clock = StallAwareStopwatch.start();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> true, cancelled, 60_000L);

    assertThat(ready).as("an already-ready transport must return ready").isTrue();
    clock.assertGaveUpWithin(1_000L, "returning at once when already ready from waiting");
  }

  @Test
  void returnsPromptlyWhenAlreadyCancelled() {
    final AtomicBoolean cancelled = new AtomicBoolean(true);

    final StallAwareStopwatch clock = StallAwareStopwatch.start();
    final boolean ready = ArcadeDbGrpcService.awaitTransportReady(() -> false, cancelled, 60_000L);

    assertThat(ready).as("a cancelled stream must not report ready").isFalse();
    clock.assertGaveUpWithin(1_000L, "a cancelled stream aborting from waiting for the deadline");
  }
}
