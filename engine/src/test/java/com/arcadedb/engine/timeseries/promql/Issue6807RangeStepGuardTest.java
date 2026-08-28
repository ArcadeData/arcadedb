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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6807: the {@code query_range} step-count guard was computed in signed 64-bit
 * arithmetic, so a span wider than {@code Long.MAX_VALUE} milliseconds wrapped negative, the guard passed,
 * and the per-step loop ran unbounded - wedging the calling thread (an Undertow worker) for the life of the
 * process.
 * <p>
 * The evaluator rejects every one of these before touching the database, so a {@code null} database is
 * enough to drive them. The {@code @Timeout} is a hang detector, not a latency bound: the pre-fix code did
 * not fail these assertions, it never returned at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6807RangeStepGuardTest {

  private static final PromQLExpr UP = new PromQLParser("up").parse();

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void spanWiderThanLongIsRejectedInsteadOfLoopingForever() {
    // The exact repro from the issue: start=-9e15s, end=9e15s, step=60s. Both operands are in order, so the
    // endMs < startMs test does not fire, and (endMs - startMs) overflows to a negative number.
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, -9_000_000_000_000_000_000L,
        9_000_000_000_000_000_000L, 60_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("wider than the representable time span");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void spanOfLongMaxWithStepOneIsRejectedInsteadOfSilentlyEmpty() {
    // spanMs / stepMs is Long.MAX_VALUE here, so the historical "+ 1" would itself have overflowed to
    // Long.MIN_VALUE and turned the rejection into an empty result. The guard is tested before the "+ 1".
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, Long.MAX_VALUE, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximum of");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void representableButTooManyStepsIsStillRejected() {
    // The pre-existing guard must keep working unchanged for a range that does not overflow.
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, 2_000_000_000L, 1_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximum of 1000000");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void endBeforeStartIsStillRejected() {
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 1_000L, 0L, 1_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be >=");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void nonPositiveStepIsStillRejected() {
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, 1_000L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stepMs must be positive");
  }
}
