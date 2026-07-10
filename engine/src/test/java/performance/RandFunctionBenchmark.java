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
package performance;

import com.arcadedb.function.math.RandFunction;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Measures the per-call cost of the SQL {@code rand()} function. The function uses a thread-local
 * {@link java.security.SecureRandom} so that the CodeQL {@code java/insecure-randomness} taint from
 * {@code rand()} into security sinks stays clear; this benchmark quantifies the overhead versus the
 * insecure {@link ThreadLocalRandom} and {@link Math#random()} alternatives.
 */
@Tag("benchmark")
class RandFunctionBenchmark {
  private static final int      N          = 5_000_000;
  private static final int      WARMUP     = 5;
  private static final int      ITERATIONS = 10;
  // Reused (not allocated per call) so the empty-args setup does not add noise to the measured loop.
  private static final Object[] NO_ARGS    = new Object[0];
  private static       long     sink;

  @Test
  void compareRandomSources() {
    final RandFunction rand = new RandFunction();

    System.out.println("\n=== rand() per-call cost: SecureRandom (current) vs ThreadLocalRandom vs Math.random() ===");

    bench("RandFunction (ThreadLocal<SecureRandom>)", () -> ((Number) rand.execute(NO_ARGS, null)).doubleValue());
    bench("ThreadLocalRandom.nextDouble()", () -> ThreadLocalRandom.current().nextDouble());
    bench("Math.random()", Math::random);
  }

  private static void bench(final String name, final DoubleOp op) {
    for (int w = 0; w < WARMUP; w++) {
      double s = 0;
      for (int i = 0; i < N; i++)
        s += op.next();
      blackhole(s);
    }

    long best = Long.MAX_VALUE;
    for (int it = 0; it < ITERATIONS; it++) {
      final long t0 = System.nanoTime();
      double s = 0;
      for (int i = 0; i < N; i++)
        s += op.next();
      final long t1 = System.nanoTime();
      blackhole((long) s);
      if (t1 - t0 < best)
        best = t1 - t0;
    }
    System.out.printf(Locale.ROOT, "%-42s best=%7.2f ms   (%.2f ns/op)%n", name, best / 1_000_000.0, (double) best / N);
  }

  private interface DoubleOp {
    double next();
  }


  private static void blackhole(final double v) {
    sink ^= (long) v;
  }

  private static void blackhole(final long v) {
    sink ^= v;
  }
}
