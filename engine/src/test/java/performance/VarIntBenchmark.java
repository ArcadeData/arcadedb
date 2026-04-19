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

import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.SplittableRandom;

/**
 * Benchmarks ArcadeDB's variable-length integer serialization ({@link Binary#putUnsignedNumber(long)}, {@link Binary#getUnsignedNumber()}) on value
 * distributions that approximate real serialization workloads:
 * <ul>
 *   <li>SMALL: 80% &lt; 128 (1-byte varints) - property counts, small ids, short string lengths</li>
 *   <li>MEDIUM: 15% in [128..16383] (2-byte varints) - most property IDs, page offsets within a page</li>
 *   <li>LARGE: 5% in [16384..2^30] (3-5 byte varints) - page numbers, RID offsets, large counts</li>
 * </ul>
 * Also measures a worst-case uniform-large distribution to stress the slow path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class VarIntBenchmark {
  private static final int N          = 1_000_000;
  private static final int WARMUP     = 5;
  private static final int ITERATIONS = 10;

  @Test
  void realisticDistribution() {
    System.out.println("\n=== Realistic distribution (80% 1-byte, 15% 2-byte, 5% 3-5 byte) ===");
    final long[] values = generateRealistic(N, 42L);
    runEncodeDecode(values);
  }

  @Test
  void uniformSmall() {
    System.out.println("\n=== All 1-byte values (0..127) ===");
    final long[] values = new long[N];
    final SplittableRandom r = new SplittableRandom(7);
    for (int i = 0; i < N; i++)
      values[i] = r.nextInt(128);
    runEncodeDecode(values);
  }

  @Test
  void uniformMedium() {
    System.out.println("\n=== All 2-byte values (128..16383) ===");
    final long[] values = new long[N];
    final SplittableRandom r = new SplittableRandom(8);
    for (int i = 0; i < N; i++)
      values[i] = 128 + r.nextInt(16256);
    runEncodeDecode(values);
  }

  @Test
  void uniformLarge() {
    System.out.println("\n=== All 5-byte values (2^28..2^35, worst-common case) ===");
    final long[] values = new long[N];
    final SplittableRandom r = new SplittableRandom(9);
    for (int i = 0; i < N; i++)
      values[i] = (1L << 28) + r.nextLong(1L << 34);
    runEncodeDecode(values);
  }

  @Test
  void numberSpace() {
    System.out.println("\n=== getUnsignedNumberSpace(value) - computes encoded length ===");
    final long[] values = generateRealistic(N, 42L);

    for (int i = 0; i < WARMUP; i++) {
      long s = 0;
      for (final long v : values) s += Binary.getUnsignedNumberSpace(v);
      blackhole(s);
    }

    long best = Long.MAX_VALUE;
    for (int i = 0; i < ITERATIONS; i++) {
      final long t0 = System.nanoTime();
      long s = 0;
      for (final long v : values) s += Binary.getUnsignedNumberSpace(v);
      final long t1 = System.nanoTime();
      blackhole(s);
      if (t1 - t0 < best) best = t1 - t0;
    }
    System.out.printf(Locale.ROOT, "getUnsignedNumberSpace  best=%6.2f ms   (%.2f ns/op)%n", best / 1_000_000.0, (double) best / N);
  }

  // --- core ------------------------------------------------------------------

  private static void runEncodeDecode(final long[] values) {
    // Pre-measure worst-case buffer size
    int maxBytes = 0;
    for (final long v : values) maxBytes += Binary.getUnsignedNumberSpace(v);

    // Encode benchmark
    final Binary buf = new Binary(maxBytes);
    for (int w = 0; w < WARMUP; w++) {
      buf.rewind();
      for (final long v : values) buf.putUnsignedNumber(v);
    }

    long bestEncode = Long.MAX_VALUE;
    for (int i = 0; i < ITERATIONS; i++) {
      buf.rewind();
      final long t0 = System.nanoTime();
      for (final long v : values) buf.putUnsignedNumber(v);
      final long t1 = System.nanoTime();
      if (t1 - t0 < bestEncode) bestEncode = t1 - t0;
    }

    // Decode benchmark - replay the buffer from position 0
    for (int w = 0; w < WARMUP; w++) {
      buf.position(0);
      long sum = 0;
      for (int i = 0; i < values.length; i++) sum += buf.getUnsignedNumber();
      blackhole(sum);
    }

    long bestDecode = Long.MAX_VALUE;
    long decodeSink = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      buf.position(0);
      final long t0 = System.nanoTime();
      long sum = 0;
      for (int j = 0; j < values.length; j++) sum += buf.getUnsignedNumber();
      final long t1 = System.nanoTime();
      decodeSink += sum;
      if (t1 - t0 < bestDecode) bestDecode = t1 - t0;
    }
    blackhole(decodeSink);

    System.out.printf(Locale.ROOT, "encode  best=%6.2f ms   (%.2f ns/op)%n", bestEncode / 1_000_000.0, (double) bestEncode / N);
    System.out.printf(Locale.ROOT, "decode  best=%6.2f ms   (%.2f ns/op)%n", bestDecode / 1_000_000.0, (double) bestDecode / N);
  }

  private static long[] generateRealistic(final int n, final long seed) {
    final long[] values = new long[n];
    final SplittableRandom r = new SplittableRandom(seed);
    for (int i = 0; i < n; i++) {
      final int bucket = r.nextInt(100);
      if (bucket < 80)
        values[i] = r.nextInt(128);
      else if (bucket < 95)
        values[i] = 128 + r.nextInt(16256);
      else
        values[i] = (1L << 20) + r.nextLong(1L << 28);
    }
    return values;
  }

  @SuppressWarnings("unused") private static long sink;

  private static void blackhole(final long v) { sink ^= v; }
}
