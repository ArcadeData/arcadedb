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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

/**
 * Compares the three paths available for packing primitive ints / longs into a {@code byte[]}:
 * <ul>
 *   <li>{@link Binary} (what ArcadeDB's serializers use today - delegates to {@link ByteBuffer})</li>
 *   <li>Raw {@link ByteBuffer#wrap(byte[])} with explicit big-endian order (what Binary wraps)</li>
 *   <li>{@link VarHandle#byteArrayViewVarHandle(Class, ByteOrder)} (direct byte[] access, no wrapper)</li>
 * </ul>
 * The goal is to decide whether rewriting {@link Binary} to VarHandle would pay off, or whether the existing ByteBuffer path is already at the speed limit.
 * Tagged {@code benchmark}, excluded from default CI.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class BinaryPrimitiveAccessBenchmark {
  private static final int N           = 1_000_000;
  private static final int WARMUP      = 5;
  private static final int ITERATIONS  = 10;
  private static final VarHandle INT_BE  = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle LONG_BE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  @Test
  void writeAndReadInts() {
    System.out.println("\n=== Write + read " + N + " ints back-to-back ===");
    final int payloadBytes = N * 4;

    measure("Binary            ", () -> {
      final Binary b = new Binary(payloadBytes);
      for (int i = 0; i < N; i++) b.putInt(i * 4, i);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += b.getInt(i * 4);
      return sum;
    });

    measure("ByteBuffer (wrap) ", () -> {
      final ByteBuffer bb = ByteBuffer.wrap(new byte[payloadBytes]).order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < N; i++) bb.putInt(i * 4, i);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += bb.getInt(i * 4);
      return sum;
    });

    measure("VarHandle (byte[])", () -> {
      final byte[] data = new byte[payloadBytes];
      for (int i = 0; i < N; i++) INT_BE.set(data, i * 4, i);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += (int) INT_BE.get(data, i * 4);
      return sum;
    });
  }

  @Test
  void writeAndReadLongs() {
    System.out.println("\n=== Write + read " + N + " longs back-to-back ===");
    final int payloadBytes = N * 8;

    measure("Binary            ", () -> {
      final Binary b = new Binary(payloadBytes);
      for (int i = 0; i < N; i++) b.putLong(i * 8, (long) i * 1_000_003L);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += b.getLong(i * 8);
      return sum;
    });

    measure("ByteBuffer (wrap) ", () -> {
      final ByteBuffer bb = ByteBuffer.wrap(new byte[payloadBytes]).order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < N; i++) bb.putLong(i * 8, (long) i * 1_000_003L);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += bb.getLong(i * 8);
      return sum;
    });

    measure("VarHandle (byte[])", () -> {
      final byte[] data = new byte[payloadBytes];
      for (int i = 0; i < N; i++) LONG_BE.set(data, i * 8, (long) i * 1_000_003L);
      long sum = 0;
      for (int i = 0; i < N; i++) sum += (long) LONG_BE.get(data, i * 8);
      return sum;
    });
  }

  // --- plumbing ---------------------------------------------------------------

  private static void measure(final String label, final java.util.function.LongSupplier op) {
    for (int i = 0; i < WARMUP; i++) blackhole(op.getAsLong());
    long best = Long.MAX_VALUE;
    long sink = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      final long t0 = System.nanoTime();
      sink += op.getAsLong();
      final long t1 = System.nanoTime();
      if (t1 - t0 < best) best = t1 - t0;
    }
    blackhole(sink);
    System.out.printf(Locale.ROOT, "%s  best=%7.2f ms   (%.2f ns/op)%n", label, best / 1_000_000.0, (double) best / N);
  }

  @SuppressWarnings("unused") private static long sink;

  private static void blackhole(final long v) { sink ^= v; }
}
