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
package com.arcadedb.server;

import com.arcadedb.integration.importer.ImporterContext;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7483: {@code ServerControlPlane.scheduleImportCounters()} used to resolve the {@code AtomicLong} behind
 * {@code ImporterContext#parsed} once and sample it directly. That counter is per-phase - it is reset to zero by
 * {@code ImporterContext#beginPhase()} at every phase boundary of a multi-phase import (documents, then vertices,
 * then edges) - so a client watching the SSE/{@code GET /api/v1/progress} stream saw the reported {@code parsed}
 * figure climb, drop back towards zero at each boundary, then climb again.
 * <p>
 * {@code ImporterContext#getParsedTotal()} is the monotonic import-wide equivalent (phases already finished plus
 * the one still running), so {@code parsedTotalSupplier()} now prefers it, falling back to the raw {@code parsed}
 * field only for a server running against an {@code arcadedb-integration} build that predates it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7483ImportProgressCounterMonotonicTest {

  @Test
  void parsedTotalSupplierReadsTheMonotonicImportWideTotal() throws Exception {
    final ImporterContext context = new ImporterContext();
    context.parsed.set(500);

    final LongSupplier supplier = parsedTotalSupplier(context);
    assertThat(supplier.getAsLong()).isEqualTo(500);

    // End of the first phase: the raw per-phase counter resets to 0 here - this is exactly the moment the SSE
    // stream used to report a value smaller than the one before it.
    context.beginPhase();
    final long afterFirstPhaseBoundary = supplier.getAsLong();
    assertThat(afterFirstPhaseBoundary)
        .as("a phase boundary must not lose progress already reported")
        .isEqualTo(500);

    context.parsed.addAndGet(10);
    assertThat(supplier.getAsLong())
        .as("the second phase's own progress is added on top of the first phase's total")
        .isEqualTo(510);

    // A second phase boundary: same check from the other side, now with two phases already folded in.
    context.beginPhase();
    assertThat(supplier.getAsLong())
        .as("a phase boundary rolls 'parsed' into the accumulator; the reported total must never go backwards")
        .isGreaterThanOrEqualTo(afterFirstPhaseBoundary);
  }

  /**
   * A stand-in for an {@code ImporterContext} predating issues #7342/#7483: no {@code getParsedTotal()}, just the
   * raw per-phase field. {@code parsedTotalSupplier()} must still report something rather than schedule nothing.
   */
  @Test
  void fallsBackToTheRawPerPhaseCounterOnAnOlderIntegrationBuild() throws Exception {
    final LegacyImporterContext legacy = new LegacyImporterContext();
    legacy.parsed.set(42);

    assertThat(parsedTotalSupplier(legacy).getAsLong()).isEqualTo(42);
  }

  public static final class LegacyImporterContext {
    public final AtomicLong parsed = new AtomicLong();
  }

  private static LongSupplier parsedTotalSupplier(final Object context) throws Exception {
    final Method method = ServerControlPlane.class.getDeclaredMethod("parsedTotalSupplier", Class.class, Object.class);
    method.setAccessible(true);
    return (LongSupplier) method.invoke(null, context.getClass(), context);
  }
}
