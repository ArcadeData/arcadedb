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
package com.arcadedb.server.monitor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8856: the policies behave exactly as the JDK ones they stand in for, and count only the saturations of a
 * running executor - a refusal by a stopped one is the owner stopping, not an undersized pool.
 */
class CountingRejectionPolicyTest {

  @Test
  void abortThrowsAndCountsWhileRunning() {
    final CountingRejectionPolicy policy = CountingRejectionPolicy.abort();
    final ThreadPoolExecutor executor = newExecutor(policy);
    try {
      assertThat(policy.isCallerRuns()).isFalse();
      assertThatThrownBy(() -> policy.rejectedExecution(() -> {
      }, executor)).isInstanceOf(RejectedExecutionException.class);
      assertThat(policy.getSaturations()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }

    assertThatThrownBy(() -> policy.rejectedExecution(() -> {
    }, executor)).isInstanceOf(RejectedExecutionException.class);
    assertThat(policy.getSaturations()).as("a stopped executor's refusal is not counted").isEqualTo(1);
  }

  @Test
  void callerRunsRunsAndCountsWhileRunningAndDiscardsOnceStopped() {
    final CountingRejectionPolicy policy = CountingRejectionPolicy.callerRuns();
    final ThreadPoolExecutor executor = newExecutor(policy);
    final AtomicBoolean ran = new AtomicBoolean();
    try {
      assertThat(policy.isCallerRuns()).isTrue();
      policy.rejectedExecution(() -> ran.set(true), executor);
      assertThat(ran.get()).isTrue();
      assertThat(policy.getSaturations()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }

    ran.set(false);
    policy.rejectedExecution(() -> ran.set(true), executor);
    assertThat(ran.get()).as("a stopped caller-runs executor discards, as the JDK policy does").isFalse();
    assertThat(policy.getSaturations()).isEqualTo(1);
  }

  private static ThreadPoolExecutor newExecutor(final CountingRejectionPolicy policy) {
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), policy);
  }
}
